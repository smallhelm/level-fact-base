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
  var tConf = {
    db: mkDB(),
    nextId: mkNextId()
  }
  var tr0 = Transactor(tConf)
  var fb = await tr0.snap()
  t.is(fb.txn, 1, 'schema transacted')
  t.is(await dbDump(fb, true), `
aveto|_s/attr|_s/attr|id1|1|true
aveto|_s/attr|_s/txn-time|id3|1|true
aveto|_s/attr|_s/type|id2|1|true
aveto|_s/type|Date|id3|1|true
aveto|_s/type|String|id1|1|true
aveto|_s/type|String|id2|1|true
eavto|id1|_s/attr|_s/attr|1|true
eavto|id1|_s/type|String|1|true
eavto|id2|_s/attr|_s/type|1|true
eavto|id2|_s/type|String|1|true
eavto|id3|_s/attr|_s/txn-time|1|true
eavto|id3|_s/type|Date|1|true
teavo|1|id1|_s/attr|_s/attr|true
teavo|1|id1|_s/type|String|true
teavo|1|id2|_s/attr|_s/type|true
teavo|1|id2|_s/type|String|true
teavo|1|id3|_s/attr|_s/txn-time|true
teavo|1|id3|_s/type|Date|true
vaeto|Date|_s/type|id3|1|true
vaeto|String|_s/type|id1|1|true
vaeto|String|_s/type|id2|1|true
vaeto|_s/attr|_s/attr|id1|1|true
vaeto|_s/txn-time|_s/attr|id3|1|true
vaeto|_s/type|_s/attr|id2|1|true
  `.trim())
  var error = await t.throws(tr0.transact([{name: 'bob'}]))
  t.is(error + '', 'Error: Fact tuple missing `$e`')

  error = await t.throws(tr0.transact([{$e: 123, name: 'bob'}]))
  t.is(error + '', 'TypeError: EntityID `$e` should be a String')

  fb = await tr0.transact([])
  fb = await tr0.transact([{$e: 'A0'}])
  t.is(fb.txn, 1, 'nothing actually transacted')

  tConf.schema = {
    name: {type: 'String'},
    email: {type: 'String'},
    foo: {type: 'String'}
  }
  var tr1 = Transactor(tConf)

  fb = await tr1.transact([
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
  t.is(fb.txn, 3, 'txn 3 finished')

  fb = await tr1.transact([{
    $e: 'BB',
    foo: 'baz'
  }])
  t.is(fb.txn, 4, 'txn 4 finished')

  t.is(await dbDump(fb), `
aveto|email|some@email|AA|3|true
aveto|foo|bar|BB|3|true
aveto|foo|baz|BB|4|true
aveto|name|bob|AA|3|true
eavto|AA|email|some@email|3|true
eavto|AA|name|bob|3|true
eavto|BB|foo|bar|3|true
eavto|BB|foo|baz|4|true
teavo|3|AA|email|some@email|true
teavo|3|AA|name|bob|true
teavo|3|BB|foo|bar|true
teavo|4|BB|foo|baz|true
vaeto|bar|foo|BB|3|true
vaeto|baz|foo|BB|4|true
vaeto|bob|name|AA|3|true
vaeto|some@email|email|AA|3|true
  `.trim())

  // Try a cold start with the same schema
  var tr2 = Transactor(tConf)
  fb = await tr2.snap()
  t.is(fb.txn, 4, 'loaded the txn')

  fb = await tr2.asOf(2)
  t.is(fb.txn, 2, 'back in time')
})

test('Schema setup', async function (t) {
  var db = mkDB()
  var nextId = mkNextId()
  var tr = Transactor({
    db: db,
    schema: {
      username: {type: 'String'}
    },
    nextId: nextId
  })

  var fb = await tr.snap()
  t.deepEqual(fb.schema.byAttr, {
    '_s/attr': {
      $e: 'id1',
      '_s/attr': '_s/attr',
      '_s/type': 'String'
    },
    '_s/type': {
      $e: 'id2',
      '_s/attr': '_s/type',
      '_s/type': 'String'
    },
    '_s/txn-time': {
      $e: 'id3',
      '_s/attr': '_s/txn-time',
      '_s/type': 'Date'
    },
    'username': {
      $e: 'id4',
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
    $e: 'id5',
    '_s/attr': 'email',
    '_s/type': 'String'
  })

  t.is(Object.keys(fb.schema.byAttr).join(','), '_s/attr,_s/txn-time,_s/type,email,username')
  fb = await tr.asOf(1)
  t.is(fb.txn, 1)
  t.is(Object.keys(fb.schema.byAttr).join(','), '_s/attr,_s/txn-time,_s/type,username')
})

test('Schema type assertions', async function (t) {
  var nextId = mkNextId()
  var tr = Transactor({
    db: mkDB(),
    nextId: nextId,
    schema: {
      name: {type: 'String'},
      incomplete: {},
      unsupported: {type: 'Foo'}
    }
  })

  async function tError (entity, expectedMsg) {
    entity.$e = nextId()
    var error = await t.throws(tr.transact([entity]))
    t.is(error + '', expectedMsg)
  }

  await tError({watda: '?'}, 'Error: Attribute `watda` schema not found')
  await tError({incomplete: '?'}, 'Error: Attribute `incomplete` is missing `_s/type`')
  await tError({unsupported: '?'}, 'Error: Attribute `unsupported`\'s `_s/type` "Foo" is not supported')
  await tError({name: 123}, 'TypeError: Expected a String for attribute `name`')
})
