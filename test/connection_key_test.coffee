require './helper'
conn   = Resque.connect()
assert.equal 'resque:foo:bar', conn.key('foo', 'bar')
conn.end()

conn   = resque namespace: "test"
assert.equal 'test:foo:bar', conn.key('foo', 'bar')
conn.end()

process.on 'exit', ->
  console.log '.'