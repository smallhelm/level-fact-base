var cuid = require('cuid')
var fastq = require('fastq')
var promisify = require('./promisify')

function mkFB (db, txn, schema) {
  var fb = Object.freeze({
    db: db,
    txn: txn,
    schema: schema
  })
  return fb
}

function transact (db, txn, entities, callback) {
  var schema = {}// TODO

  var facts = []
  entities.forEach(function (entity) {
    if (typeof entity !== 'object' || !entity.hasOwnProperty('$e') || typeof entity.$e !== 'string') {
      callback(new Error('Fact tuple missing `$e`'))
      return
    }
    Object.keys(entity).forEach(function (attr) {
      if (attr === '$e') return
      if (attr === '$retract') return

      var value = entity[attr]

      // TODO validate with schema

      facts.push({
        e: entity.$e,
        a: attr,
        v: value,
        t: txn,
        o: !entity.$retract,
        factId: cuid()
      })
    })
  })

  if (facts.length === 0) {
    callback()
    return
  }

  var dbOps = []

  dbOps.push({
    type: 'put',
    key: 'latest-txn',
    value: txn
  })

  facts.forEach(function (fact) {
    [
      'eavto',
      'aveto',
      'vaeto',
      'teavo'
    ].forEach(function (index) {
      dbOps.push({
        type: 'put',
        key: [index].concat(index.split('').map(function (key) {
          return fact[key]
        })),
        value: fact.factId
      })
    })
  })

  db.batch(dbOps, function (err) {
    if (err) return callback(err)
    callback(null, mkFB(db, txn, schema))
  })
}

function loadCurrFB (db, callback) {
  var schema = {}// TODO
  db.get('latest-txn', function (err, txn) {
    if (err) {
      if (err.notFound) {
        txn = 0
      } else {
        return callback(err)
      }
    }
    callback(null, mkFB(db, txn, schema))
  })
}

module.exports = function Transactor (db) {
  var currFB

  var onLoadCBs = []
  var errorLoading = null

  function onLoad (callback) {
    if (errorLoading || currFB) {
      callback(errorLoading, currFB)
      return
    }
    onLoadCBs.push(callback)
    if (onLoadCBs.length > 1) {
      return
    }

    loadCurrFB(db, function (err, fb) {
      errorLoading = err
      currFB = fb

      while (onLoadCBs.length > 0) {
        onLoadCBs.shift()(errorLoading, currFB)
      }
    })
  }

  var transactQ = fastq(function (entities, callback) {
    var txn = currFB.txn + 1
    transact(db, txn, entities, function (err, fb) {
      if (err) return callback(err)
      if (fb) {
        currFB = fb
      }
      callback(null, currFB)
    })
  })

  return {
    snap: function (callback) {
      callback = callback || promisify()
      onLoad(callback)
      return callback.promise
    },
    transact: function (entities, callback) {
      callback = callback || promisify()
      onLoad(function (err) {
        if (err) return callback(err)
        transactQ.push(entities, callback)
      })
      return callback.promise
    }
  }
}
