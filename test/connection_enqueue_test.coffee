require './helper'
calls  = 0
resque = connect()

resque.redis.scard resque.key('queues'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

resque.redis.llen resque.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

resque.enqueue 'my-queue', 'some-function'

resque.redis.scard resque.key('queues'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

resque.redis.llen resque.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

resque.redis.rpop resque.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  data = JSON.parse resp
  assert.equal 'some-function', data.class
  assert.deepEqual [],          data.args

resque.enqueue 'my-queue', 'other-function', ['abc', 1]

resque.redis.rpop resque.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  data = JSON.parse resp
  assert.equal 'other-function', data.class
  assert.deepEqual ['abc', 1],   data.args
  resque.end()

process.on 'exit', ->
  assert.equal 6, calls
  console.log '.'