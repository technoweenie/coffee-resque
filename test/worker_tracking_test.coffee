require './helper'

calls = 0
conn  = resque()

# no workers to start
conn.redis.scard conn.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

# start 2 workers
worker1 = conn.worker '*'
worker2 = conn.worker '*'

worker1.name = '1'
worker2.name = '2'

# before Worker#checkQueues has finished.
assert.equal "1", worker1.name
assert.equal "2", worker2.name

worker1.init ->
  assert.equal "1:#{process.pid}:", worker1.name
worker2.init ->
  assert.equal "2:#{process.pid}:", worker2.name

# they're in the workers set
conn.redis.scard conn.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 2, resp

# each worker has a tracked start date
conn.redis.exists conn.key('worker', worker1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

conn.redis.exists conn.key('worker', worker2.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

# set some fake stats for the workers
conn.redis.incr conn.key('stat:failed', worker1.name)

worker1.untrack()

# its not in the workers set anymore
conn.redis.scard conn.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

# stats are still available
conn.redis.exists conn.key('stat:failed', worker1.name), (err, resp) ->
  calls += 1
  assert.equal true, resp

# untracked worker still has a start date
conn.redis.exists conn.key('worker', worker1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

worker1.end()

# worker stat is gone
conn.redis.exists conn.key('stat:failed', worker1.name), (err, resp) ->
  calls += 1
  assert.equal false, resp

# worker start date is gone
conn.redis.exists conn.key('worker', worker1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal false, resp

worker2.end ->
  # every key is gone
  conn.redis.scard conn.key('workers'), (err, resp) ->
    assert.equal 0, resp
    calls += 1
    conn.end()

process.on 'exit', ->
  assert.equal 10, calls
  console.log '.'