require './helper'
calls = 0
conn  = resque()

conn.redis.scard conn.key('queues'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

conn.redis.llen conn.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

conn.enqueue 'my-queue', 'some-function'

conn.redis.scard conn.key('queues'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

conn.redis.llen conn.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

conn.redis.rpop conn.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  data = JSON.parse resp
  assert.equal 'some-function', data.class
  assert.deepEqual [],          data.args

conn.enqueue 'my-queue', 'other-function', ['abc', 1]

conn.redis.rpop conn.key('queue', 'my-queue'), (err, resp) ->
  calls += 1
  data = JSON.parse resp
  assert.equal 'other-function', data.class
  assert.deepEqual ['abc', 1],   data.args
  conn.end()

process.on 'exit', ->
  assert.equal 6, calls
  console.log '.'