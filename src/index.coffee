EventEmitter = require('events').EventEmitter

class Connection
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
    options.callbacks ||= @callbacks
    new exports.Worker @, queues, options

  # Public
  end: ->
    @redis.quit()

  key: (args...) ->
    args.unshift @namespace
    args.join ":"

class Worker extends EventEmitter
  constructor: (connection, queues, options) ->
    @conn      = connection
    @redis     = connection.redis
    @queues    = queues
    @name      = options.name      || 'node'
    @callbacks = options.callbacks || {}

  # Public
  start: ->
    @init()

  # Public
  end: ->
    @purge()

  track: ->
    @redis.sadd @conn.key('workers'), @name

  untrack: ->
    @redis.srem @conn.key('workers'), @name

  init: ->
    @track()
    @redis.set  @conn.key('worker', @name, 'started'), (new Date).toString()

  purge: ->
    @untrack()
    @redis.del [
      @conn.key('worker', @name, 'started')
      @conn.key('stat', 'failed', @name)
      @conn.key('stat', 'processed', @name)
    ]

connectToRedis = (options) ->
  require('../../node_redis').createClient options.port, options.host

exports.Connection = Connection
exports.Worker     = Worker

exports.connect = (options) ->
  new exports.Connection options || {}