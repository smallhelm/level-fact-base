var cuid = require('cuid')
var level = require('level')
var charwise = require('charwise')
var Transactor = require('level-fact-base')

var db = level('db', {
  keyEncoding: charwise, // or any codec for sorted arrays of flat json values
  valueEncoding: 'json'
})

// define a schema for attributes
// like datomic schema is stored and versioned inside the database
var schema = {
  user_name: { type: 'String' },
  user_email: { type: 'String' },

  blogpost_text: { type: 'String' },

  comment_blogpostId: { type: 'EntityID' },
  comment_userId: { type: 'EntityID' },
  comment_text: { type: 'String' }
  // ...
}
var tr = Transactor(db, schema)

async function main () {
  // create some test data
  // + 2 users
  // + a blog post
  // + 3 comments
  var fb = await tr.transact([
    {
      $e: 'user0', // in the real world you'll generate a unique id using something like cuid
      user_name: 'foo',
      user_email: 'foo@example.com'
    },
    {
      $e: 'user1',
      user_name: 'bar',
      user_email: 'bar@example.com'
    },
    {
      $e: 'post0',
      blogpost_text: 'some blog post... no so long'
    },
    {
      $e: cuid(),
      comment_userId: 'user0',
      comment_blogpostId: 'post0',
      comment_text: 'This article sucks!'
    },
    {
      $e: cuid(),
      comment_userId: 'user1',
      comment_blogpostId: 'post0',
      comment_text: 'Why? I think this article is life-changing!'
    },
    {
      $e: cuid(),
      comment_userId: 'user0',
      comment_blogpostId: 'post0',
      comment_text: 'im just a troll'
    }
  ])

  // Read an entity
  console.log(await fb.get('user0'))
  /*
  { '$e': 'user0',
    user_email: 'foo@example.com',
    user_name: 'foo'
  }
  */

  // Get user0's comments
  console.log(await fb.q(
    [
      ['?cid', 'comment_userId', '?uid'],
      ['?cid', 'comment_text', '?text']
    ],
    { uid: 'user0' },
    ['text']
  ))
  /*
  [ { text: 'This article sucks!' },
    { text: 'im just a troll' }
  ]
  */

  // Get name+email of those who commented on post0
  console.log(await fb.q(
    [
      ['?cid', 'comment_blogpostId', '?postId'],
      ['?cid', 'comment_userId', '?uid'],
      ['?uid', 'user_name', '?name'],
      ['?uid', 'user_email', '?email']
    ],
    { postId: 'post0' },
    ['name', 'email']
  ))
  /*
  [ { name: 'foo', email: 'foo@example.com' },
    { name: 'bar', email: 'bar@example.com' }
  ]
  */
}
main()
