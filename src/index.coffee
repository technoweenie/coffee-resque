EventEmitter = require('events').EventEmitter

class Connection extends EventEmitter
  constructor: (options) ->
    @redis     = options.redis     || connectToRedis options
    @namespace = options.namespace || 'resque'
    @callbacks = options.callbacks || {}
    @timeout   = options.timeout   || 100
    @redis.select options.database if options.database?

  # Public
  enqueue: (queue, func, args...) ->
    @redis.sadd  @key('queues'), queue
    @redis.rpush @key('queue', queue),
      JSON.stringify class: func, args: args

  # Public
  worker: (queues, options) ->
    options           ||= {}
    options.callbacks ||= @callbacks
    new exports.Worker @, queues, options

  # Public
  end: ->
    @redis.quit()

  key: (args...) ->
    args.unshift @namespace
    args.join ":"

class Worker
  constructor: (connection, queues, options) ->
    @conn      = connection
    @redis     = connection.redis
    @queues    = queues
    @name      = [options.name || 'node', process.pid, queues].join(":")
    @callbacks = options.callbacks || {}
    @running   = false
    @checkQueues()

  # Public
  start: ->
    @init()
    @poll()

  # Public
  end: (cb) ->
    @running = false
    @untrack()
    @redis.del [
      @conn.key('worker', @name, 'started')
      @conn.key('stat', 'failed', @name)
      @conn.key('stat', 'processed', @name)
    ], cb

  poll: ->
    return if !@running
    @queue = @queues.shift()
    @queues.push @queue
    @conn.emit 'poll', @, @queue
    @redis.lpop @conn.key('queue', @queue), (err, resp) =>
      if !err && resp
        @perform JSON.parse(resp.toString())
      else
        @conn.emit err, @, @queue if err
        @pause()

  perform: (job) ->
    @conn.emit 'job', @, @queue, job
    try 
      if cb = @callbacks[job.class]
        cb job.args...
        @succeed job
      else
        throw "Missing Job: #{job.class}"
    catch err
      @fail err, job
    finally
      @poll()

  succeed: (job) ->
    @redis.incr @conn.key('stat', 'processed')
    @redis.incr @conn.key('stat', 'processed', @name)
    @conn.emit 'success', @, @queue, job

  fail: (err, job) ->
    @redis.incr  @conn.key('stat', 'failed')
    @redis.incr  @conn.key('stat', 'failed', @name)
    @redis.rpush @conn.key('failed'),
      JSON.stringify(@failurePayload(err, job))
    @conn.emit 'error', err, @, @queue, job

  pause: ->
    @untrack()
    setTimeout =>
      return if !@running
      @track()
      @poll()
    , @conn.timeout

  track: ->
    @running = true
    @redis.sadd @conn.key('workers'), @name

  untrack: ->
    @redis.srem @conn.key('workers'), @name

  init: ->
    @track()
    @redis.set  @conn.key('worker', @name, 'started'), (new Date).toString()

  checkQueues: ->
    return if @queues.shift?
    @queues = @queues.split(',')

  failurePayload: (err, job) ->
    {
      worker: @name
      error:  err.error || 'unspecified'
      payload: job
      exception: err.exception || 'generic'
      backtrace: err.backtrace || ['unknown']
      failed_at: (new Date).toString()
    }

connectToRedis = (options) ->
  require('../../node_redis').createClient options.port, options.host

exports.Connection = Connection
exports.Worker     = Worker

exports.connect = (options) ->
  new exports.Connection options || {}