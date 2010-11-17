require './helper'

calls = 0
resque1 = connect name: 'w1'
resque2 = connect name: 'w2'
# before Worker#checkQueues has finished.
assert.equal "1", resque1.name
assert.equal "2", resque2.name

# no workers to start
resque1.redis.scard resque1.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

resque1.init ->
  assert.equal "1:#{process.pid}:", resque1.name
resque2.init ->
  assert.equal "2:#{process.pid}:", resque2.name

# they're in the workers set
resque1.redis.scard resque.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 2, resp

# each worker has a tracked start date
resque1.redis.exists resque.key('worker', resque1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

resque1.redis.exists resque.key('worker', resque2.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

# set some fake stats for the workers
resque.redis.incr resque1.key('stat:failed', resque1.name)

resque1.untrack()

# its not in the workers set anymore
resque.redis.scard resque.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 1, resp

# stats are still available
resque.redis.exists resque.key('stat:failed', worker1.name), (err, resp) ->
  calls += 1
  assert.equal true, resp

# untracked worker still has a start date
resque.redis.exists resque.key('worker', worker1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal true, resp

worker1.end()

# worker stat is gone
resque.redis.exists resque.key('stat:failed', worker1.name), (err, resp) ->
  calls += 1
  assert.equal false, resp

# worker start date is gone
resque.redis.exists resque.key('worker', worker1.name, 'started'), (err, resp) ->
  calls += 1
  assert.equal false, resp

worker2.end ->
  # every key is gone
  resque.redis.scard resque.key('workers'), (err, resp) ->
    assert.equal 0, resp
    calls += 1
    resque.end()

process.on 'exit', ->
  assert.equal 10, calls
  console.log '.'