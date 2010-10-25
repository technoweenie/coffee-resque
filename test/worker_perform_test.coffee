require './helper'
conn   = resque()

conn.enqueue 'test',  'abc', 'first'
conn.enqueue 'test',  'abc', 'fail'
conn.enqueue 'test2', 'def', 'missing'
conn.enqueue 'test',  'abc', 'last'

stats = {jobs: [], success: [], error: [], polls: 0}

conn.on 'job', (worker, queue, job) ->
  stats.jobs.push [queue, job.args[0]]
  if stats.jobs.length == 4
    worker.end()

conn.on 'success', (worker, queue, job) ->
  stats.success.push job.args[0]

conn.on 'error', (err, worker, queue, job) ->
  stats.error.push job.args[0]

conn.on 'poll', (worker, queue) ->
  stats.polls += 1

conn.callbacks.abc = (arg) ->
  if arg == 'fail'
    throw "Failing the job"

worker = conn.worker('test,test2')
worker.start()

process.on 'exit', ->
  assert.ok stats.polls > 4
  assert.deepEqual ['test',  'first'],   stats.jobs[0]
  assert.deepEqual ['test2', 'missing'], stats.jobs[1]
  assert.deepEqual ['test',  'fail'],    stats.jobs[2]
  assert.deepEqual ['test',  'last'],    stats.jobs[3]
  assert.deepEqual ['first', 'last'],    stats.success
  assert.deepEqual ['missing', 'fail'],  stats.error
  console.log '.'