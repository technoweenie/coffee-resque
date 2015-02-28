require './helper'
conn  = resque()

conn.size 'test', (err, resp)->
  assert.equal err, null
  initial_size = resp

  conn.enqueue 'test',  'abc', ['one']
  conn.enqueue 'test',  'abc', ['two']
  conn.enqueue 'test',  'abc', ['three']
  conn.enqueue 'test',  'abc', ['four']

  conn.size 'test', (err, resp)->
    assert.equal err, null
    assert.equal resp, initial_size + 4
    conn.end()

process.on 'exit', ->
  console.log '.'






