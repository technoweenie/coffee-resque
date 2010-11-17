require './helper'
resque   = connect timeout: 10

resque.enqueue 'test',  'abc', ['first']
resque.enqueue 'test',  'abc', ['fail']
resque.enqueue 'test2', 'def', ['missing']
resque.enqueue 'test',  'abc', ['second']
resque.enqueue 'test',  'abc', ['last']

stats = {jobs: [], success: [], error: [], polls: 0}

resque.on 'job', (queue, job) ->
  stats.jobs.push [queue, job.args[0]]

resque.on 'success', (queue, job) ->
  stats.success.push job.args[0]
  if stats.success.length == 3
    countStats()

resque.on 'error', (err, queue, job) ->
  stats.error.push job.args[0]

resque.on 'poll', (queue) ->
  stats.polls += 1

resque.job 'abc', (arg, next) ->
  try
    if arg == 'fail'
      throw "Failing the job"
    next()
  catch err
    next err

resque.poll('test,test2')

countStats = ->
  resque.redis.get resque.key('stat', 'failed'), (err, resp) ->
    calls += 1
    assert.equal '2', resp.toString()
  resque.redis.get resque.key('stat', 'failed', resque.name), (err, resp) ->
    calls += 1
    assert.equal '2', resp.toString()
  resque.redis.get resque.key('stat', 'processed'), (err, resp) ->
    calls += 1
    assert.equal '3', resp.toString()
  resque.redis.get resque.key('stat', 'processed', resque.name), (err, resp) ->
    calls += 1
    assert.equal '3', resp.toString()
    resque.end()

calls = 0
process.on 'exit', ->
  assert.ok stats.polls > 5
  assert.deepEqual ['test',  'first'],   stats.jobs[0]
  assert.deepEqual ['test2', 'missing'], stats.jobs[1]
  assert.deepEqual ['test',  'fail'],    stats.jobs[2]
  assert.deepEqual ['test',  'second'],  stats.jobs[3]
  assert.deepEqual ['test',  'last'],    stats.jobs[4]
  assert.deepEqual ['missing', 'fail'],  stats.error
  assert.deepEqual ['first', 'second', 'last'], stats.success
  assert.equal 4, calls
  console.log '.'