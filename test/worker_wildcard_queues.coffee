require './helper'

conn = resque()
conn.enqueue 'abc', 'def'
conn.enqueue 'def', 'ghi'

queues = []
conn.on 'error', (err, worker, queue) ->
  queues.push queue.toString()
  if queues.length == 2
    worker.end()
    conn.end()
    assert.deepEqual ['abc', 'def'], queues

conn.worker('*').start()

process.on 'exit', ->
  console.log '.'