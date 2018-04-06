var Transactor = require('./Transactor')
var charwise = require('charwise')
var dbRange = require('./dbRange')
var encode = require('encoding-down')
var levelup = require('levelup')
var memdown = require('memdown')
var test = require('ava')

function mkDB () {
  return levelup(encode(memdown(), {
    keyEncoding: charwise,
    valueEncoding: 'json'
  }))
}

test('Transactor', async function (t) {
  var db = mkDB()
  var tr = Transactor(db)

  var error = await t.throws(tr.transact([{name: 'bob'}]))
  t.is(error + '', 'Error: Fact tuple missing `$e`')

  var fb
  fb = await tr.transact([])
  fb = await tr.transact([{$e: 'A0'}])
  t.is(fb.txn, 0, 'nothing actually transacted')

  fb = await tr.transact([
    {
      $e: 'AA',
      name: 'bob',
      email: 'some@email'
    },
    {
      $e: 'BB',
      foo: 'bar'
    }
  ])
  t.is(fb.txn, 1, 'txn 1 finished')

  fb = await tr.transact([{
    $e: 'BB',
    foo: 'baz'
  }])
  t.is(fb.txn, 2, 'txn 2 finished')

  var dbStr = ''
  await dbRange(fb.db, {
    prefix: []
  }, function (data, stopRange) {
    dbStr += data.key.join('|') + '\n'
  })
  t.is(dbStr.trim(), `
aveto|email|some@email|AA|1|true
aveto|foo|bar|BB|1|true
aveto|foo|baz|BB|2|true
aveto|name|bob|AA|1|true
eavto|AA|email|some@email|1|true
eavto|AA|name|bob|1|true
eavto|BB|foo|bar|1|true
eavto|BB|foo|baz|2|true
teavo|1|AA|email|some@email|true
teavo|1|AA|name|bob|true
teavo|1|BB|foo|bar|true
teavo|2|BB|foo|baz|true
vaeto|bar|foo|BB|1|true
vaeto|baz|foo|BB|2|true
vaeto|bob|name|AA|1|true
vaeto|some@email|email|AA|1|true
  `.trim())

  // Try a cold start
  var tr2 = Transactor(db)
  fb = await tr2.snap()
  t.is(fb.txn, 2, 'loaded the txn')
})
