exports.version    = "0.1.0"

# Sets up a new Resque Connection.  This Connection can either be used to 
# queue new Resque jobs, or be passed into a worker through a `connection` 
# option.
#
# options - Optional Hash of options.
#           host      - String Redis host.  (Default: Redis' default)
#           port      - Integer Redis port.  (Default: Redis' default)
#           namespace - String namespace prefix for Redis keys.  
#                       (Default: resque).
#           timeout   - Integer timeout in milliseconds to pause polling if 
#                       the queue is empty.
#           database  - Optional Integer of the Redis database to select.
#           name      - Optional String name of the Worker.  (Default: node)
#
# Returns a Connection instance.
exports.connect = (options) ->
  new exports.Worker options || {}

EventEmitter = require('events').EventEmitter

# Handles the connection to the Redis server.  Connections also spawn worker
# instances for processing jobs.
#
# Emits 'poll' each time Redis is checked.
#   queue  - The String queue that is being checked.
#
# Emits 'job' before attempting to run any job.
#   queue  - The String queue that is being checked.
#   job    - The parsed Job object that was being run.
#
# Emits 'success' after a successful job completion.
#   queue  - The String queue that is being checked.
#   job    - The parsed Job object that was being run.
#
# Emits 'error' if there is an error fetching or running the job.
#   err    - The caught exception.
#   queue  - The String queue that is being checked.
#   job    - The parsed Job object that was being run.
class Worker extends EventEmitter
  constructor: (options) ->
    @redis     = options.redis     || connectToRedis options
    @namespace = options.namespace || 'resque'
    @callbacks = options.callbacks || {}
    @timeout   = options.timeout   || 5000
    @name      = options.name
    @running   = false
    @ready     = false
    @redis.select options.database if options.database?

  # queues - Either a comma separated String or Array of queue names.
  poll: (queues) ->
    if @ready
      @init => 
        @pop()
    else
      @checkQueues queues

  job: (job_name, func) ->
    @on "job:#{job_name}", (job, next) ->
      func job.args..., next

  # Public: Queues a job in a given queue to be run.
  #
  # queue - String queue name.
  # func  - String name of the function to run.
  # args  - Optional Array of arguments to pass.
  #
  # Returns nothing.
  enqueue: (queue, func, args) ->
    @redis.sadd  @key('queues'), queue
    @redis.rpush @key('queue', queue),
      JSON.stringify class: func, args: args || []

  # Public: Quits the connection to the Redis server.
  #
  # Returns nothing.
  end: (cb) ->
    if @running
      @cleanup =>
        cb() if cb
        @redis.end()
    else
      cb() if cb
      @redis.end()

  # PRIVATE METHODS

  # Polls the next queue for a job.
  #
  # Returns nothing.
  pop: ->
    return if !@running
    @queue = @queues.shift()
    @queues.push @queue
    @emit 'poll', @queue
    @redis.lpop @key('queue', @queue), (err, resp) =>
      if !err && resp
        @perform JSON.parse(resp.toString())
      else
        @emit 'error', err, @queue if err
        @pause()

  # Handles the actual running of the job.
  #
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
  perform: (job) ->
    old_title = process.title
    @emit 'job', @queue, job
    @procline "#{@queue} job since #{(new Date).toString()}"

    has_listener = 
      @emit "job:#{job.class}", job, (err) =>
        if err
          @fail err, job
        else
          @succeed job
        process.title = old_title
        @pop()

    if !has_listener
      @fail {error: "No listener for #{job.class}"}, job
      @pop()

  # Tracks stats for successfully completed jobs.
  #
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
  succeed: (job) ->
    @redis.incr @key('stat', 'processed')
    @redis.incr @key('stat', 'processed', @name)
    @emit 'success', @queue, job

  # Tracks stats for failed jobs, and tracks them in a Redis list.
  #
  # err - The caught Exception.
  # job - The parsed Job object that is being run.
  #
  # Returns nothing.
  fail: (err, job) ->
    @redis.incr  @key('stat', 'failed')
    @redis.incr  @key('stat', 'failed', @name)
    @redis.rpush @key('failed'),
      JSON.stringify(@failurePayload(err, job))
    @emit 'error', err, @queue, job

  # Pauses polling if no jobs are found.  Polling is resumed after the timeout
  # has passed.
  #
  # Returns nothing.
  pause: ->
    @untrack()
    @procline "Sleeping for #{@timeout/1000}s"
    setTimeout =>
      return if !@running
      @track()
      @poll()
    , @timeout

  # Tracks this worker's name in Redis.
  #
  # Returns nothing.
  track: ->
    @running = true
    @redis.sadd @key('workers'), @name

  # Removes this worker's name from Redis.
  #
  # Returns nothing.
  untrack: ->
    @redis.srem @key('workers'), @name

  cleanup: (cb) ->
    @running = false
    @untrack()
    args = [[
      @key('worker', @name, 'started')
      @key('stat', 'failed', @name)
      @key('stat', 'processed', @name)
    ]]
    args.push cb if cb
    @redis.del args...

  # Initializes this Worker's start date in Redis.
  #
  # Returns nothing.
  init: (cb) ->
    @track()
    args = [@key('worker', @name, 'started'), (new Date).toString()]
    @procline "Processing #{@queues.toString} since #{args.last}"
    args.push cb if cb
    @redis.set args...

  # Ensures that the given @queues value is in the right format.
  #
  # Returns nothing.
  checkQueues: (queues) ->
    @queues = queues
    return if @queues.shift? # if it's an array, we're all set
    if @queues == '*'
      @redis.smembers @key('queues'), (err, resp) =>
        @queues = []
        if resp
          resp.sort().forEach (q) =>
            @queues.push q.toString()

        @ready  = true
        @name   = @_name
        @poll() if @running
    else
      @queues = @queues.split(',')
      @ready  = true
      @name   = @_name
      @poll()

  # Sets the process title.
  #
  # msg - The String message for the title.
  #
  # Returns nothing.
  procline: (msg) ->
    process.title = "resque-#{exports.version}: #{msg}"

  # Builds a payload for the Resque failed list.
  #
  # err - The caught Exception.
  # job - The parsed Job object that is being run.
  #
  # Returns a Hash.
  failurePayload: (err, job) ->
    worker:    @name
    error:     err.error or 'unspecified'
    payload:   job
    exception: err.exception or 'generic'
    backtrace: err.backtrace or ['unknown']
    failed_at: (new Date).toString()

  Object.defineProperty @prototype, 'name',
    get: -> @_name
    set: (name) ->
      @_name = if @ready
        [name or 'node', process.pid, @queues].join(":")
      else
        name
      @_name

  # Builds a namespaced Redis key with the given arguments.
  #
  # args - Array of Strings.
  #
  # Returns an assembled String key.
  key: (args...) ->
    args.unshift @namespace
    args.join ":"

connectToRedis = (options) ->
  require('redis').createClient options.port, options.host

exports.Worker = Worker