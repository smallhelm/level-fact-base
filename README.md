# level-fact-base
Store "facts" in level and query them via datalog

# Why "Facts"??

## Anatomy of a fact
A fact is something that happened.

 * `E` - ***Entity*** - an id that represents an entity i.e. `"user10"`
 * `A` - ***Attribute*** - the attribute the fact is about i.e. `"email"`
 * `V` - ***Value*** - the attribute value i.e. `"some@email"`
 * `T` - ***Time*** - when this fact became true
 * `O` - ***Assertion/Retraction*** - whether the fact was asserted or retracted

## The power of facts
Using a fact based information model inherently provide these 3 benefits.

### 1 - Flexible data model
Only storing facts makes adapting to changing requirements easy.

### 2 - Built in, queryable history and auditing
Most databases only remember the last thing that was true. For example

```txt
 - April 1 -
user10 : Set email to "old@email"

 - April 6 -
user10 : Set email to "new@email"

 - April 18 -
You    : What is the user10's email address?
aDumbDB: "new@email"

You    : What was user10's email on April 3?
aDumbDB: "new@email"
```
What you really want is this:
```txt
You     : What is the user10's email address?
FactBase: "new@email", on April 6 it was set by user10

You     : What was user10's email on April 3?
FactBase: "old@email", on April 1 it was set by user10, but on April 6 it was unset by user10
```

### 3 - Easy to query with performant joins
Joins are very powerful and can make your life easier. If your database doesn't provide them, then you need to do them by hand. The way they are commonly implemented in SQL style databases can cause performance headaches. They can also be a pain to write and understand.

Using a fact based data model and datalog makes joins not only easy to express, but they are naturally performant.

Lets say you want all of the blog comments for user 10.
```sql
SELECT
  c.id,
  c.text
FROM
  users u
  JOIN comment ON c.user_id = u.id
WHERE
  u.id = 10
```
Contrast that with the level-fact-base datalog equivalent
```js
[["?id", "user/id"     , 10     ],
 ["?id", "comment/text", "?text"]]
```

# Inspired by Datomic, but not Datomic
If you haven't heard about [Datomic](http://www.datomic.com/), go read about it now!

level-fact-base is not a re-implementation or a clone of Datomic. However, its information model and use of datalog are inspired by Datomic.

# API

The API is fairly stable. Once this project is a v1.x.x release it will follow semver so breaking API changes will be noted and expressed by incrementing the major version number.

```js
//the writer
var Transactor = require("level-fact-base/transactor");

//the connection
var Connection = require("level-fact-base/connection");

//query functions
var q          = require("level-fact-base/q");
var qTuple     = require("level-fact-base/qTuple");
var getEntity  = require("level-fact-base/getEntity");
```


## Transactor(db[, options], onStartup)
 * `db` is any thing that exposes a levelup api.
 * `options.hindex` by default it's [level-hash-index](https://github.com/smallhelm/level-hash-index), but you can pass in your own thing that exposes that api.
 * `onStartup(err, transactor)` is called once the transactor is warm and ready to go
 * `transactor.connection` is an instance of the Connection object
 * `transactor.transact` is the `transact` function

### transact(fact\_tuples[, tx\_data], callback)
This is the only function for making writes to level-fact-base. `tx_data` are attributes and values that will be expanded to the transaction tuples. This is useful for retaining information about the transaction itself.

```js
transact([["10", "user/email", "my@email"],
          ["10", "user/name" , "bob"     ]],

         {"performed/by": "10"},

         function(err, fb){
           //fb is the latest fb version
         })
```

## Connection(db[, options], callback)
 * `db` is any thing that exposes a levelup api.
 * `options.hindex` by default it's [level-hash-index](https://github.com/smallhelm/level-hash-index), but you can pass in your own thing that exposes that api.
 * `callback(err, connection)` is called once the connection is ready

### fb = connection.snap()
Get a snapshot of the database.

### fb = connection.asOf(txn\_id, callback)
Get the database at a particular transaction id
 * `txn_id` the transaction number you wish to get a snapshot of
 * `callback(err, fb)` the fb value at that `txn_id`

## q(fb, tuples[, bindings], callback)
The main entry point for performing datalog queries. As you'll notice it's just javascript arrays. Anything that starts with `"?"` is considered a variable. `"?_"` is the throw away variable (not bound to anything)

```js
q(fb, [["?id", "user/id"     , "?user_id"],
       ["?id", "comment/text", "?text"   ]],

      [{"?user_id": 10}],

      function(err, r){

        //r is [
        //  {"?user_id": 10, "?id": 123, "?text": "some comment about the post..."},
        //  {"?user_id": 10, "?id": 321, "?text": "annother comment"},
        //  ...
        //];

      });
```
To help prevent injection attacks only use strings, numbers, and booleans inside the query. Don't put variables in the query, pass them in as bindings. This way they can be properly checked and escaped.

## qTuple(fb, tuples[, binding], callback)
`q` is built upon this function. It is called for every tuple in `q` and for each of `q`'s bindings.

```js
qTuple(fb, ["?id", "user/id", "?user_id"],

           {"?user_id": 10},

           function(err, r){

             //r is [
             //  {"?user_id": 10, "?id": 123},
             //  {"?user_id": 10, "?id": 321},
             //  ...
             //];

           });
```

## getEntity(fb, e, callback)
A sugar function that simply gets all attributes and values for `e`.

```js
getEntity(fb, 10, function(err, user){

  // user is {id: 10, name: "bob", email: "my@email"}

});
```

# License

The MIT License (MIT)

Copyright (c) 2015 Small Helm LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
