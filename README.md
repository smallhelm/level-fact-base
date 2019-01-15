# level-fact-base

[![Build Status](https://travis-ci.org/smallhelm/level-fact-base.svg?branch=master)](https://travis-ci.org/smallhelm/level-fact-base)
[![stability - experimental](https://img.shields.io/badge/stability-experimental-orange.svg)](https://nodejs.org/api/documentation.html#documentation_stability_index)
[![JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)

Store immutable "facts" in [level](https://github.com/Level/level) and query them with datalog.

level-fact-base is inspired by [Datomic](http://www.datomic.com/). Also check out [datascript](https://github.com/tonsky/datascript).

## Why "Facts"??

A fact is something that happened.

 * `E` - ***Entity*** - an id that represents an entity i.e. `"user10"`
 * `A` - ***Attribute*** - the attribute the fact is about i.e. `"email"`
 * `V` - ***Value*** - the attribute value i.e. `"some@email"`
 * `T` - ***Transaction*** - when this fact became true
 * `O` - ***Assertion/Retraction*** - whether the fact was asserted or retracted

### The power of facts
Using a fact based information model inherently provide these 3 benefits.

#### 1 - Flexible data model

When you write data you don't need to think much about how it will be queried later. You simply assert attributes about an entity. Facts are not tightly coupled with structure.

#### 2 - Built in, queryable history and auditing
Most databases only remember the last thing that was true. For example:

```txt
 - April 1 -
user10 : Set email to "old@email"

 - April 6 -
user10 : Set email to "new@email"

 - April 18 -
You    : What is user10's email address?
aDumbDB: "new@email"

You    : What was user10's email on April 3?
aDumbDB: "new@email"
```

But what you really want is this:

```txt
You     : What is user10's email address?
FactBase: "new@email", on April 6 it was set by user10

You     : What was user10's email on April 3?
FactBase: "old@email", on April 1 it was set by user10, but on April 6 it was changed to "new@email" by user10
```

#### 3 - Easy to query with performant joins

Fact base joins are implicit, it simply matches binding variables and unions results. The database is fully indexed for you so you don't need to worry about primary keys or indexes.

For example in SQL:
```sql
SELECT
  c.id,
  c.text
FROM
  users u
  JOIN comment c ON c.userId = u.id
WHERE
  u.email = 'my@email'
```

The fact datalog equivalent:

```js
[
  ['?uid', 'user_email'    , 'my@email'],
  ['?cid', 'comment_userId', '?uid'    ],
  ['?cid', 'comment_text'  , '?text'   ]
]
// implicitly joined on ?uid and ?cid

```


## API

```js
var Transactor = require('level-fact-base')

var db = level('db', {
  keyEncoding: require('charwise'),// or bytewise, or any codec for sorted arrays of flat json values
  valueEncoding: 'json'
})

// define a schema for attributes
// like datomic schema is stored and versioned inside the database
var schema = {
  user_name: {type: 'String'},
  user_email: {type: 'String'},
  comment_userId: {type: 'EntityID'}
  comment_text: {type: 'String'}
  // ...
}

var tr = Transactor(db, schema)

// like levelup, every asynchronous function either takes a callback or returns a promise
// i.e. a callback
tr.snap(function(err, fb){ ... })
// or return a Promise
var fb = await tr.snap()
```

Checkout [example/index.js](https://github.com/smallhelm/level-fact-base/blob/master/example/index.js) for a more complete example.

### tr = Transactor(db, initSchema)

Initialize the fact-base and return a transactor (`tr` for short)

 * `db` is any thing that exposes a levelup api.
 * `initSchema` the current expected schema for the transactor to use. As part of starting up the transactor it will sync up the schema to match what you pass it.

#### tr.snap() -> fb

Asynchronously get the current snapshot of the database.

#### tr.asOf(txn) -> fb

Asynchronously get a given `txn` version of the database.

#### tr.transact(entities) -> fb

Assert facts and get the resulting new version of the database.

```js
transact([
  {
    $e: '101', // the entity id
    email: 'my@email',
    name: 'bob',
    // This expands to:
    // ['101', 'email', 'my@email']
    // ['101', 'name' , 'bob'     ]
  }
], function(err, fb){
  // fb is the new fb version
})

// or
fb = await transact([..])
```

#### fb.q(tuples, binding, select) -> results

The main entry point for performing datalog queries. Anything that starts with `'?'` is a binding variable.

```js
fb.q([[ '?uid', 'user_email'    , '?email' ],
      [ '?cid', 'comment_userId', '?uid'   ],
      [ '?cid', 'comment_text'  , '?text'  ]

      { email: 'my@email' }, // map of bindings i.e. bind ?email to 'my@email'

      [ 'cid', 'text' ], // select which result bindings we care about

      function(err, results){
        // results are
        // [
        //   {cid: '123', text: 'some comment about the post...'},
        //   {cid: '321', text: 'annother comment'},
        //   ...
        // ]
      })

// or
results = await fb.q([..], {..}, [..])
```


You may also pass filter functions as the values in a binding map. Bound functions should return a boolean and filter out facts that evalutate falsy. 


```js
function hasExclamation (text) {
  return text.includes('!')
}

fb.q([[ '?cid', 'comment_userId', '?uid'   ],
      [ '?cid', 'comment_text'  , '?text'  ]

      { text: hasExclamation }, // match comments on text value

      [ 'cid', 'text' ], // select which result bindings we care about

      function(err, results){
        // results are
        // [
        //   {cid: '456', text: 'wow!'},
        //   {cid: '654', text: 'super!'},
        //   ...
        // ]
      })
```

For more examples see `test.js`.


NOTE: To help prevent injection attacks, use bindings to pass in untrusted data so it's properly escaped.


#### fb.get($e) -> entity
A sugar function that simply gets all attributes an entity.

```js
fb.get('101', function(err, user){

  // user is {$e: '101', name: 'bob', email: 'my@email'}

})

// or
user = await fb.get('101')
```

## License
MIT
