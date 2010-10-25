class Connection
  constructor: (options) ->
    @redis     = options.redis     || connectToRedis options
    @namespace = options.namespace || 'resque'
    @redis.select options.database if options.database?

  enqueue: (queue, func, args...) ->
    @redis.sadd  @key('queues'), queue
    @redis.rpush @key('queue', queue),
      JSON.stringify class: func, args: args

  key: (args...) ->
    args.unshift @namespace
    args.join ":"

  end: ->
    @redis.quit()

connectToRedis = (options) ->
  require('../../node_redis').createClient options.port, options.host

exports.Connection = Connection

exports.connect = (options) ->
  new exports.Connection options || {}