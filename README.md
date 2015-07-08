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
dumb DB: "new@email"

You    : What was user10's email on April 3?
dumb DB: "new@email"
```
What you really want is this:
```txt
You     : What is the user10's email address?
factBase: "new@email", true as of April 6 according to user10

You     : What was user10's email on April 3?
factBase: "old@email", true from April 1 until April 6 according to user10
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
[['?id', 'user/id'     , 10     ],
 ['?id', 'comment/text', '?text']]
```

# Inspired by Datomic, but not Datomic
If you haven't heard about [Datomic](http://www.datomic.com/), go read about it now!

level-fact-base is not a re-implementation or a clone of Datomic. However, its information model and use of datalog are inspired by Datomic.

# API

TODO

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
