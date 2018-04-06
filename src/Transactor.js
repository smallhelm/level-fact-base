var λ = require('contra')
var cuid = require('cuid')
var dbRange = require('./dbRange')
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

var systemSchema = {
  '_s/type': {
    '_s/type': 'String'
  },
  '_s/txn-time': {
    '_s/type': 'Date'
  }
}

function parseFact (dbKey) {
  var fact = {}
  dbKey[0].split('').forEach(function (key, i) {
    fact[key] = dbKey[i + 1]
  })
  return fact
}

function getEntity (db, txn, id, callback) {
  var facts = []
  dbRange(db, {
    prefix: ['eavto', id]
  }, function (data) {
    if (data.key[4] > txn) {
      return// too new
    }
    facts.push(parseFact(data.key))
  }, function (err) {
    if (err) return callback(err)
    facts.sort(function (a, b) {
      return a.t - b.t
    })
    var entity = {}
    facts.forEach(function (fact) {
      if (fact.o) {
        entity[fact.a] = fact.v
      } else {
        delete entity[fact.a]
      }
    })
    callback(null, entity)
  })
}

function loadSchemaAsOf (db, txn, callback) {
  var attrIds = []
  dbRange(db, {
    prefix: ['aveto', '_s/attr']
  }, function (data) {
    if (data.key[4] > txn) {
      return// too new
    }
    attrIds.push(data.key[3])
  }, function (err) {
    if (err) return callback(err)

    var schema = {}
    λ.each(attrIds, function (id, next) {
      getEntity(db, txn, id, function (err, entity) {
        if (err) return next(err)
        schema[id] = entity
        next()
      })
    }, function (err) {
      callback(err, schema)
    })
  })
}

function loadCurrFB (db, callback) {
  db.get('latest-txn', function (err, txn) {
    if (err) {
      if (err.notFound) {
        txn = 0
      } else {
        return callback(err)
      }
    }
    loadSchemaAsOf(db, txn, function (err, schema) {
      if (err) return callback(err)
      callback(null, mkFB(db, txn, schema))
    })
  })
}

module.exports = function Transactor (db) {
  var currFB

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

  function syncSchema (callback) {
    var currSchema = currFB.schema
    var goalSchema = systemSchema

    var toTransact = []

    Object.keys(currSchema).forEach(function (attrId) {
      if (!goalSchema[attrId]) {
        // remove the old val
        toTransact.push(Object.assign({}, currSchema[attrId], {
          $e: attrId,
          $retract: true
        }))
      }
    })

    Object.keys(goalSchema).forEach(function (attrId) {
      var entity = {
        $e: attrId,
        '_s/attr': attrId
      }
      var foundDiff = false
      Object.keys(goalSchema[attrId]).forEach(function (attr) {
        if (attr === '_s/attr') return
        var goalV = goalSchema[attrId][attr]
        var currV = currSchema[attrId] && currSchema[attrId][attr]
        if (goalV !== currV) {
          entity[attr] = goalSchema[attrId][attr]
          foundDiff = true
        }
      })
      if (foundDiff) {
        toTransact.push(entity)
      }
    })
    transactQ.push(toTransact, callback)
  }

  var onLoadCBs = []
  var doneLoading = false
  var errorLoading = null

  var callOnLoadCBs = function (err) {
    errorLoading = err
    doneLoading = true
    while (onLoadCBs.length > 0) {
      onLoadCBs.shift()(errorLoading, currFB)
    }
  }

  function onLoad (callback) {
    if (errorLoading || doneLoading) {
      callback(errorLoading, currFB)
      return
    }
    onLoadCBs.push(callback)
    if (onLoadCBs.length > 1) {
      return
    }

    loadCurrFB(db, function (err, fb) {
      if (err) return callOnLoadCBs(err)
      currFB = fb
      syncSchema(callOnLoadCBs)
    })
  }

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
