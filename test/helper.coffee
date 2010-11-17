GLOBAL.assert ||= require 'assert'
GLOBAL.Resque ||= require '../src'
GLOBAL.connect   = (options) ->
  options           ||= {}
  options.namespace ||= 'coffee-resque-test'
  conn = Resque.connect options
  conn.redis.flushdb()
  conn