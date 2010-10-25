GLOBAL.assert ||= require 'assert'
GLOBAL.Resque ||= require '../src'
GLOBAL.resque   = (options) ->
  options           ||= {}
  options.namespace ||= 'coffee-resque-test'
  conn = Resque.connect options
  conn.redis.flushdb()
  conn

GLOBAL.worker   = (queues, options) ->
  options           ||= {}
  options.namespace ||= 'coffee-resque-test'
  worker = Resque.worker queues, options
  worker.redis.flushdb()
  worker