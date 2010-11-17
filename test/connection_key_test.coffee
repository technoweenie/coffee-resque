require './helper'
resque   = Resque.connect()
assert.equal 'resque:foo:bar', resque.key('foo', 'bar')
resque.end()

resque   = connect namespace: "test"
assert.equal 'test:foo:bar', resque.key('foo', 'bar')
resque.end()

process.on 'exit', ->
  console.log '.'