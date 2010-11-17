require './helper'

calls = 0
resque1 = connect name: 'w1'
resque2 = connect name: 'w2'
spare_redis   = resque2.redis
resque2.redis = resque1.redis

# before Worker#check_queues has finished.
assert.equal "w1", resque1.name
assert.equal "w2", resque2.name

resque1.enqueue 'queue1', 'func'
resque2.enqueue 'queue2', 'func'

# no workers to start
resque1.redis.scard resque1.key('workers'), (err, resp) ->
  calls += 1
  assert.equal 0, resp

r1_check_pid = ->
  assert.equal "w1:#{process.pid}:queue1", resque1.name
  check_both_init()
  resque1.removeListener 'poll', r1_check_pid
r2_check_pid = ->
  assert.equal "w2:#{process.pid}:queue2", resque2.name
  check_both_init()
  resque2.removeListener 'poll', r2_check_pid

resque1.on 'poll', r1_check_pid
resque2.on 'poll', r2_check_pid

resque1.job 'func', (next) -> next()
resque2.job 'func', (next) -> next()

resque1.poll 'queue1'
resque2.poll 'queue2'

both_init = false
check_both_init = ->
  if !both_init
    return both_init = true

  # they're in the workers set
  resque1.redis.scard resque1.key('workers'), (err, resp) ->
    calls += 1
    assert.equal 2, resp

  # each worker has a tracked start date
  resque1.redis.exists resque1.key('worker', resque1.name, 'started'), (err, resp) ->
    calls += 1
    assert.equal true, resp

  resque1.redis.exists resque1.key('worker', resque2.name, 'started'), (err, resp) ->
    calls += 1
    assert.equal true, resp

  # set some fake stats for the workers
  resque1.redis.incr resque1.key('stat:failed', resque1.name)

  resque1.untrack()

  # its not in the workers set anymore
  resque1.redis.scard resque1.key('workers'), (err, resp) ->
    calls += 1
    assert.equal 1, resp

  # stats are still available
  resque1.redis.exists resque1.key('stat:failed', resque1.name), (err, resp) ->
    calls += 1
    assert.equal true, resp

  # untracked worker still has a start date
  resque1.redis.exists resque1.key('worker', resque1.name, 'started'), (err, resp) ->
    calls += 1
    assert.equal true, resp

  resque1.cleanup()

  # worker stat is gone
  resque1.redis.exists resque1.key('stat:failed', resque1.name), (err, resp) ->
    calls += 1
    assert.equal false, resp

  # worker start date is gone
  resque1.redis.exists resque1.key('worker', resque1.name, 'started'), (err, resp) ->
    calls += 1
    assert.equal false, resp

    # END will terminate the redis connection, even if there are some queued
    # requests waiting for responses. 
    resque2.end ->
      # every key is gone
      spare_redis.scard resque1.key('workers'), (err, resp) ->
        assert.equal 0, resp
        calls += 1
        spare_redis.end()

process.on 'exit', ->
  assert.equal 10, calls
  console.log '.'