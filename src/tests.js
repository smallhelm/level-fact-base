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

var mkNextId = function () {
  var currId = 0
  return function () {
    currId++
    return 'id' + currId
  }
}

async function dbDump (fb, keepSchema) {
  var dbStr = ''
  await dbRange(fb.db, {
    prefix: []
  }, function (data, stopRange) {
    if (!keepSchema) {
      if (/^_s\//.test(data.key[data.key[0].indexOf('a') + 1])) {
        return
      }
    }
    dbStr += data.key.join('|') + '\n'
  })
  return dbStr.trim()
}

test('Transactor', async function (t) {
  var db = mkDB()
  var tr = Transactor({db: db, nextId: mkNextId()})
  var fb = await tr.snap()
  t.is(fb.txn, 1, 'schema transacted')
  t.is(await dbDump(fb, true), `
aveto|_s/attr|_s/txn-time|id2|1|true
aveto|_s/attr|_s/type|id1|1|true
aveto|_s/type|Date|id2|1|true
aveto|_s/type|String|id1|1|true
eavto|id1|_s/attr|_s/type|1|true
eavto|id1|_s/type|String|1|true
eavto|id2|_s/attr|_s/txn-time|1|true
eavto|id2|_s/type|Date|1|true
teavo|1|id1|_s/attr|_s/type|true
teavo|1|id1|_s/type|String|true
teavo|1|id2|_s/attr|_s/txn-time|true
teavo|1|id2|_s/type|Date|true
vaeto|Date|_s/type|id2|1|true
vaeto|String|_s/type|id1|1|true
vaeto|_s/txn-time|_s/attr|id2|1|true
vaeto|_s/type|_s/attr|id1|1|true
  `.trim())

  var error = await t.throws(tr.transact([{name: 'bob'}]))
  t.is(error + '', 'Error: Fact tuple missing `$e`')

  fb = await tr.transact([])
  fb = await tr.transact([{$e: 'A0'}])
  t.is(fb.txn, 1, 'nothing actually transacted')

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
  t.is(fb.txn, 2, 'txn 2 finished')

  fb = await tr.transact([{
    $e: 'BB',
    foo: 'baz'
  }])
  t.is(fb.txn, 3, 'txn 3 finished')

  t.is(await dbDump(fb), `
aveto|email|some@email|AA|2|true
aveto|foo|bar|BB|2|true
aveto|foo|baz|BB|3|true
aveto|name|bob|AA|2|true
eavto|AA|email|some@email|2|true
eavto|AA|name|bob|2|true
eavto|BB|foo|bar|2|true
eavto|BB|foo|baz|3|true
teavo|2|AA|email|some@email|true
teavo|2|AA|name|bob|true
teavo|2|BB|foo|bar|true
teavo|3|BB|foo|baz|true
vaeto|bar|foo|BB|2|true
vaeto|baz|foo|BB|3|true
vaeto|bob|name|AA|2|true
vaeto|some@email|email|AA|2|true
  `.trim())

  // Try a cold start
  var tr2 = Transactor({db: db, nextId: mkNextId()})
  fb = await tr2.snap()
  t.is(fb.txn, 3, 'loaded the txn')
})

test('Schema', async function (t) {
  var db = mkDB()
  var nextId = mkNextId()
  var tr = Transactor({
    db: db,
    schema: {
      username: {
        type: 'String'
      }
    },
    nextId: nextId
  })

  var fb = await tr.snap()
  t.deepEqual(fb.schema.byAttr, {
    '_s/type': {
      $e: 'id1',
      '_s/attr': '_s/type',
      '_s/type': 'String'
    },
    '_s/txn-time': {
      $e: 'id2',
      '_s/attr': '_s/txn-time',
      '_s/type': 'Date'
    },
    'username': {
      $e: 'id3',
      '_s/attr': 'username',
      '_s/type': 'String'
    }
  })

  fb = await tr.transact([{
    $e: nextId(),
    '_s/attr': 'email',
    '_s/type': 'String'
  }])

  t.deepEqual(fb.schema.byAttr.email, {
    $e: 'id4',
    '_s/attr': 'email',
    '_s/type': 'String'
  })
})
