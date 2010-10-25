EventEmitter = require('events').EventEmitter

class Connection extends EventEmitter
  constructor: (options) ->
    @redis     = options.redis     || connectToRedis options
    @namespace = options.namespace || 'resque'
    @callbacks = options.callbacks || {}
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
  end: ->
    @running = false
    @untrack()
    @redis.del [
      @conn.key('worker', @name, 'started')
      @conn.key('stat', 'failed', @name)
      @conn.key('stat', 'processed', @name)
    ], (err) => @redis.end()

  poll: ->
    return if !@running
    @queue = @queues.shift()
    @queues.push @queue
    @conn.emit 'poll', @, @queue
    @redis.lpop @conn.key('queue', @queue), (err, resp) =>
      if resp
        @perform JSON.parse(resp.toString())
      else
        @pause()

  perform: (job) ->
    @conn.emit 'job', @, @queue, job
    try 
      if cb = @callbacks[job.class]
        cb job.args...
        @conn.emit 'success', @, @queue, job
      else
        throw "Missing Job: #{job.class}"
    catch err
      @conn.emit 'error', err, @, @queue, job
    finally
      @poll()

  pause: ->
    @untrack()
    setTimeout =>
      return if !@running
      @track()
      @poll()
    , 1000

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

connectToRedis = (options) ->
  require('../../node_redis').createClient options.port, options.host

exports.Connection = Connection
exports.Worker     = Worker

exports.connect = (options) ->
  new exports.Connection options || {}