EventEmitter = require('events').EventEmitter

# Handles the connection to the Redis server.  Connections also spawn worker
# instances for processing jobs.
class Connection extends EventEmitter
  constructor: (options) ->
    @redis     = options.redis     || connectToRedis options
    @namespace = options.namespace || 'resque'
    @callbacks = options.callbacks || {}
    @timeout   = options.timeout   || 100
    @redis.select options.database if options.database?

  # Public: Queues a job in a given queue to be run.
  #
  # queue - String queue name.
  # func  - String name of the function to run.
  # args  - Optional Array of arguments to pass.
  #
  # Returns nothing.
  enqueue: (queue, func, args...) ->
    @redis.sadd  @key('queues'), queue
    @redis.rpush @key('queue', queue),
      JSON.stringify class: func, args: args

  # Public: Creates a single Worker from this Connection.
  #
  # queues  - Either a comma separated String or Array of queue names.
  # options - Optional Hash of options.
  #           name      - String name of the Worker.  (Default: node)
  #           callbacks - Optional Object that has the job functions defined.
  #                       This will be taken from the Connection by default.
  #
  # Returns a Worker instance.
  worker: (queues, options) ->
    options           ||= {}
    options.callbacks ||= @callbacks
    new exports.Worker @, queues, options

  # Public: Quits the connection to the Redis server.
  #
  # Returns nothing.
  end: ->
    @redis.quit()

  # Builds a namespaced Redis key with the given arguments.
  #
  # args - Array of Strings.
  #
  # Returns an assembled String key.
  key: (args...) ->
    args.unshift @namespace
    args.join ":"

# Handles the queue polling and job running.
class Worker
  # See Connection#worker
  constructor: (connection, queues, options) ->
    @conn      = connection
    @redis     = connection.redis
    @queues    = queues
    @name      = [options.name || 'node', process.pid, queues].join(":")
    @callbacks = options.callbacks || {}
    @running   = false
    @checkQueues()

  # Public: Tracks the worker in Redis and starts polling.
  #
  # Returns nothing.
  start: ->
    @init => @poll()

  # Public: Stops polling and purges this Worker's stats from Redis.
  # 
  # cb - Optional Function callback.
  #
  # Returns nothing.
  end: (cb) ->
    @running = false
    @untrack()
    @redis.del [
      @conn.key('worker', @name, 'started')
      @conn.key('stat', 'failed', @name)
      @conn.key('stat', 'processed', @name)
    ], cb

  # Polls the next queue for a job.  Events are emitted directly on the 
  # Connection instance.
  #
  # Emits 'poll' each time Redis is checked.
  #   err    - The caught exception.
  #   worker - This Worker instance.
  #   queue  - The String queue that is being checked.
  #
  # Emits 'job' before attempting to run any job.
  #   worker - This Worker instance.
  #   queue  - The String queue that is being checked.
  #   job    - The parsed Job object that was being run.
  #
  # Emits 'success' after a successful job completion.
  #   worker - This Worker instance.
  #   queue  - The String queue that is being checked.
  #   job    - The parsed Job object that was being run.
  #
  # Emits 'error' if there is an error fetching or running the job.
  #   err    - The caught exception.
  #   worker - This Worker instance.
  #   queue  - The String queue that is being checked.
  #   job    - The parsed Job object that was being run.
  #
  # Returns nothing.
  poll: ->
    return if !@running
    @queue = @queues.shift()
    @queues.push @queue
    @conn.emit 'poll', @, @queue
    @redis.lpop @conn.key('queue', @queue), (err, resp) =>
      if !err && resp
        @perform JSON.parse(resp.toString())
      else
        @conn.emit 'error', err, @, @queue if err
        @pause()

  # Handles the actual running of the job.
  #
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
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

  # Tracks stats for successfully completed jobs.
  #
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
  succeed: (job) ->
    @redis.incr @conn.key('stat', 'processed')
    @redis.incr @conn.key('stat', 'processed', @name)
    @conn.emit 'success', @, @queue, job

  # Tracks stats for failed jobs, and tracks them in a Redis list.
  #
  # err - The caught Exception.
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
  fail: (err, job) ->
    @redis.incr  @conn.key('stat', 'failed')
    @redis.incr  @conn.key('stat', 'failed', @name)
    @redis.rpush @conn.key('failed'),
      JSON.stringify(@failurePayload(err, job))
    @conn.emit 'error', err, @, @queue, job

  # Pauses polling if no jobs are found.  Polling is resumed after the timeout
  # has passed.
  #
  # Returns nothing.
  pause: ->
    @untrack()
    setTimeout =>
      return if !@running
      @track()
      @poll()
    , @conn.timeout

  # Tracks this worker's name in Redis.
  #
  # Returns nothing.
  track: ->
    @running = true
    @redis.sadd @conn.key('workers'), @name

  # Removes this worker's name from Redis.
  #
  # Returns nothing.
  untrack: ->
    @redis.srem @conn.key('workers'), @name

  # Initializes this Worker's start date in Redis.
  #
  # Returns nothing.
  init: (cb) ->
    @track()
    args = [@conn.key('worker', @name, 'started'), (new Date).toString()]
    args.push cb if cb
    @redis.set args...

  # Ensures that the given @queues value is in the right format.
  #
  # Returns nothing.
  checkQueues: ->
    return if @queues.shift?
    @queues = @queues.split(',')

  # Builds a payload for the Resque failed list.
  #
  # err - The caught Exception.
  # job - The parsed Job object that is being run.
  #
  # Returns a Hash.
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

# Sets up a new Worker.
#
# queues  - Either a comma separated String or Array of queue names.
# options - Optional Hash of options.
#           host      - String Redis host.  (Default: Redis' default)
#           port      - Integer Redis port.  (Default: Redis' default)
#           namespace - String namespace prefix for Redis keys.  
#                       (Default: resque).
#           timeout   - Integer timeout in milliseconds to pause polling if 
#                       the queue is empty.
#           name      - String name of the Worker.  (Default: node).
#           callbacks - Optional Object that has the job functions defined.
#                       This will be taken from the Connection by default.
#
# Returns a Worker instance.
exports.worker = (queues, options) ->
  conn = exports.connect options
  conn.worker queues, options

# Sets up a new Resque Connection.
#
# options - Optional Hash of options.
#           host      - String Redis host.  (Default: Redis' default)
#           port      - Integer Redis port.  (Default: Redis' default)
#           namespace - String namespace prefix for Redis keys.  
#                       (Default: resque).
#           timeout   - Integer timeout in milliseconds to pause polling if 
#                       the queue is empty.
#           name      - String name of the Worker.  (Default: node).
#
# Returns a Connection instance.
exports.connect = (options) ->
  new exports.Connection options || {}