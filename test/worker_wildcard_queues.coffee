require './helper'

resque = connect()
resque.enqueue 'abc', 'def'
resque.enqueue 'def', 'ghi'

queues = []
resque.on 'job', (queue, job) ->
  queues.push queue.toString()
  if queues.length == 2
    resque.end()
    assert.deepEqual ['abc', 'def'], queues

resque.poll('*')

process.on 'exit', ->
  console.log '.'