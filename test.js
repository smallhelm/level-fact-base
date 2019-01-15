var charwise = require('charwise')
var dbRange = require('./src/dbRange')
var encode = require('encoding-down')
var levelup = require('levelup')
var memdown = require('memdown')
var test = require('ava')
var Transactor = require('./')

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
  var nextId = mkNextId()
  var schema = {}
  var tr0 = Transactor(db, schema, nextId)
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
  var error = await t.throwsAsync(tr0.transact([{ name: 'bob' }]))
  t.is(error + '', 'Error: Fact tuple missing `$e`')

  error = await t.throwsAsync(tr0.transact([{ $e: 123, name: 'bob' }]))
  t.is(error + '', 'TypeError: EntityID `$e` should be a String')

  fb = await tr0.transact([])
  fb = await tr0.transact([{ $e: 'A0' }])
  t.is(fb.txn, 1, 'nothing actually transacted')

  schema = {
    name: { type: 'String' },
    email: { type: 'String' },
    foo: { type: 'String' }
  }
  var tr1 = Transactor(db, schema, nextId)

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
  var tr2 = Transactor(db, schema, nextId)
  fb = await tr2.snap()
  t.is(fb.txn, 4, 'loaded the txn')

  fb = await tr2.asOf(2)
  t.is(fb.txn, 2, 'back in time')
})

test('Schema setup', async function (t) {
  var nextId = mkNextId()
  var tr = Transactor(mkDB(), {
    username: { type: 'String' }
  }, nextId)

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
    $e: 'id13',
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
  var tr = Transactor(mkDB(), {
    name: { type: 'String' },
    incomplete: {},
    unsupported: { type: 'Foo' }
  }, nextId)

  async function tError (entity, expectedMsg) {
    entity.$e = nextId()
    var error = await t.throwsAsync(tr.transact([entity]))
    t.is(error + '', expectedMsg)
  }

  await tError({ watda: '?' }, 'Error: Attribute `watda` schema not found')
  await tError({ incomplete: '?' }, 'Error: Attribute `incomplete` is missing `_s/type`')
  await tError({ unsupported: '?' }, 'Error: Attribute `unsupported`\'s `_s/type` "Foo" is not supported')
  await tError({ name: 123 }, 'TypeError: Expected a String for attribute `name`')
})

test('q', async function (t) {
  var tr = Transactor(mkDB(), {
    father: { type: 'String' },
    mother: { type: 'String' }
  }, mkNextId())

  var fb = await tr.transact([
    { $e: 'axl', father: 'mike', mother: 'frankie' },
    { $e: 'sue', father: 'mike', mother: 'frankie' },
    { $e: 'brick', father: 'mike', mother: 'frankie' },
    { $e: 'mike', father: 'big mike' },
    { $e: 'rusty', father: 'big mike' },
    { $e: 'frankie', father: 'tag', mother: 'pat' },
    { $e: 'janet', father: 'tag', mother: 'pat' }
  ])

  var data = await fb.q([['?id', 'father', '?dad']])
  t.deepEqual(data, [
    { id: 'mike', dad: 'big mike' },
    { id: 'rusty', dad: 'big mike' },
    { id: 'axl', dad: 'mike' },
    { id: 'brick', dad: 'mike' },
    { id: 'sue', dad: 'mike' },
    { id: 'frankie', dad: 'tag' },
    { id: 'janet', dad: 'tag' }
  ])

  data = await fb.q([['?id', 'father', '?dad']], {
    dad: 'mike'
  })
  t.deepEqual(data, [
    { id: 'axl', dad: 'mike' },
    { id: 'brick', dad: 'mike' },
    { id: 'sue', dad: 'mike' }
  ])

  data = await fb.q([['?id', 'father', '?dad']], {
    dad: 'mike'
  }, ['id'])
  t.deepEqual(data, [
    { id: 'axl' },
    { id: 'brick' },
    { id: 'sue' }
  ])

  data = await fb.q([
    ['?child', 'father', '?husband'],
    ['?child', 'mother', '?wife']
  ], {}, ['husband', 'wife'])
  t.deepEqual(data, [
    { husband: 'mike', wife: 'frankie' },
    { husband: 'tag', wife: 'pat' }
  ])
})

test('get', async function (t) {
  var tr = Transactor(mkDB(), {
    name: { type: 'String' },
    email: { type: 'String' }
  }, mkNextId())

  var fb = await tr.transact([
    { $e: 'aaa', name: 'jim', email: 'a@a.a' },
    { $e: 'bbb', email: 'b@b.b' }
  ])

  t.deepEqual(await fb.get('aaa'), {
    $e: 'aaa',
    name: 'jim',
    email: 'a@a.a'
  })

  t.deepEqual(await fb.get('bbb'), {
    $e: 'bbb',
    email: 'b@b.b'
  })

  var fbNew = await tr.transact([
    { $e: 'aaa', name: 'a name change' }
  ])

  t.deepEqual(await fb.get('aaa'), {
    $e: 'aaa',
    name: 'jim',
    email: 'a@a.a'
  })
  t.deepEqual(await fbNew.get('aaa'), {
    $e: 'aaa',
    name: 'a name change',
    email: 'a@a.a'
  })
})

test('Date', async function (t) {
  var tr = Transactor(mkDB(), {
    jsDate: { type: 'Date' }
  }, mkNextId())

  var fb = await tr.transact([
    { $e: 'aaa', jsDate: new Date(1111) },
    { $e: 'bbb', jsDate: new Date(2222) },
    { $e: 'ccc', jsDate: new Date(1111) }
  ])

  t.deepEqual(await fb.get('aaa'), {
    $e: 'aaa',
    jsDate: (new Date(1111)).toISOString()
  })
  var data = await fb.q([
    ['?id', 'jsDate', '?d']
  ], { d: new Date(1111) }, ['id'])

  t.deepEqual(data, [
    { id: 'aaa' },
    { id: 'ccc' }
  ])
})

test('Function binding', async function (t) {
  var tr = Transactor(mkDB(), {
    price: { type: 'Integer' }
  }, mkNextId())

  var fb = await tr.transact([
    { $e: 'apple', price: 9 },
    { $e: 'banana', price: 2 },
    { $e: 'canteloupe', price: 4 },
    { $e: 'durian', price: 3 },
    { $e: 'elderberry', price: 1 },
    { $e: 'feijoa', price: 2 },
    { $e: 'grapes', price: 1 }
  ])

  function gt3 (price) {
    return price > 3
  }

  var data = await fb.q(
    [['?id', 'price', '?p']],
    { p: gt3 },
    ['id', 'p']
  )

  t.deepEqual(data, [
    { id: 'canteloupe', p: 4 },
    { id: 'apple', p: 9 }
  ])
})

test('Multiple function binding', async function (t) {
  var tr = Transactor(mkDB(), {
    price: { type: 'Integer' },
    quantity: { type: 'Integer' }
  }, mkNextId())

  var fb = await tr.transact([
    { $e: 'apple', price: 9, quantity: 0 },
    { $e: 'banana', price: 2, quantity: 2 },
    { $e: 'canteloupe', price: 4, quantity: 0 },
    { $e: 'durian', price: 3, quantity: 8 },
    { $e: 'elderberry', price: 1, quantity: 0 },
    { $e: 'feijoa', price: 2, quantity: 1 },
    { $e: 'grapes', price: 1, quantity: 3 }
  ])

  function lt4 (price) {
    return price < 4
  }

  function inStock (quantity) {
    return quantity > 0
  }

  var data = await fb.q(
    [
      ['?id', 'price', '?p'],
      ['?id', 'quantity', '?q']
    ],
    { p: lt4, q: inStock },
    ['id', 'p', 'q']
  )

  t.deepEqual(data, [
    { id: 'grapes', p: 1, q: 3 },
    { id: 'banana', p: 2, q: 2 },
    { id: 'feijoa', p: 2, q: 1 },
    { id: 'durian', p: 3, q: 8 }
  ])
})
