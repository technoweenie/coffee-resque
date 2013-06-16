require './helper'
conn   = resque timeout: 10

conn.enqueue 'test',  'abc', ['first']
conn.enqueue 'test',  'abc', ['fail']
conn.enqueue 'test2', 'def', ['missing']
conn.enqueue 'test',  'abc', ['second']
conn.enqueue 'test',  'abc', ['last']

stats = {jobs: [], success: [], error: [], polls: 0}

conn.callbacks.abc = (arg, callback) ->
  if arg is 'fail'
    callback new Error "Failing the job"
  else
    callback 'pass'

worker = conn.worker('test,test2')

worker.on 'job', (worker, queue, job) ->
  stats.jobs.push [queue, job.args[0]]

worker.on 'success', (worker, queue, job) ->
  stats.success.push job.args[0]

worker.on 'error', (err, worker, queue, job) ->
  stats.error.push job.args[0]

worker.on 'poll', (worker, queue) ->
  stats.polls += 1
  if stats.polls == 7
    countStats()

worker.start()

countStats = ->
  conn.redis.get conn.key('stat', 'failed'), (err, resp) ->
    calls += 1
    assert.equal '2', resp.toString()
  conn.redis.get conn.key('stat', 'failed', worker.name), (err, resp) ->
    calls += 1
    assert.equal '2', resp.toString()
  conn.redis.get conn.key('stat', 'processed'), (err, resp) ->
    calls += 1
    assert.equal '3', resp.toString()
  conn.redis.get conn.key('stat', 'processed', worker.name), (err, resp) ->
    calls += 1
    assert.equal '3', resp.toString()
    worker.end -> conn.end()

calls = 0
process.on 'exit', ->
  assert.ok stats.polls > 5
  assert.deepEqual ['test',  'first'],   stats.jobs[0]
  assert.deepEqual ['test',  'fail'],    stats.jobs[1]
  assert.deepEqual ['test',  'second'],  stats.jobs[2]
  assert.deepEqual ['test',  'last'],    stats.jobs[3]
  assert.deepEqual ['test2', 'missing'], stats.jobs[4]
  assert.deepEqual ['fail', 'missing'],  stats.error
  assert.deepEqual ['first', 'second', 'last'], stats.success
  assert.equal 4, calls
  console.log '.'