var eachSeries = require('async/eachSeries')
var cuid = require('cuid')
var dbRange = require('./dbRange')
var fastq = require('fastq')
var promisify = require('./promisify')
var schemaTypes = require('./schemaTypes')
var q = require('./q')
var get = require('./get')

function mkFB (db, txn, schema) {
  var fb = Object.freeze({
    db: db,
    txn: txn,
    schema: schema,
    q: q,
    get: get
  })
  return fb
}

var systemSchema = Object.freeze({
  '_s/attr': {
    '_s/type': 'String'
  },
  '_s/type': {
    '_s/type': 'String'
  },
  '_s/txn-time': {
    '_s/type': 'Date'
  }
})

function getSchemaFor (fb, attr) {
  if (fb.schema.byAttr[attr]) {
    return fb.schema.byAttr[attr]
  }
  if (systemSchema[attr]) {
    return systemSchema[attr]
  }
}

function transact (db, fb, entities, nextId, callback) {
  var txn = fb.txn + 1

  var schemaChanged = false

  var facts = []
  entities.forEach(function (entity) {
    if (typeof entity !== 'object' || !entity.hasOwnProperty('$e')) {
      throw new Error('Fact tuple missing `$e`')
    }
    if (!schemaTypes.EntityID.validate(entity.$e)) {
      throw new TypeError('EntityID `$e` should be a String')
    }

    Object.keys(entity).forEach(function (attr) {
      if (attr === '$e') return
      if (attr === '$retract') return

      if (attr === '_s/attr' || fb.schema.byId[entity.$e]) {
        schemaChanged = true
      }

      var value = entity[attr]

      var sc = getSchemaFor(fb, attr)
      if (!sc) {
        throw new Error('Attribute `' + attr + '` schema not found')
      }
      var type = sc['_s/type']
      if (!type) {
        throw new Error('Attribute `' + attr + '` is missing `_s/type`')
      }
      const attrType = schemaTypes[type]
      if (!attrType) {
        throw new Error('Attribute `' + attr + '`\'s `_s/type` "' + type + '" is not supported')
      }
      if (!attrType.validate(value)) {
        throw new TypeError('Expected a ' + type + ' for attribute `' + attr + '`')
      }
      if (attrType.encode) {
        value = attrType.encode(value)
      }

      facts.push({
        e: entity.$e,
        a: attr,
        v: value,
        t: txn,
        o: !entity.$retract,
        factId: nextId()
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

    if (schemaChanged) {
      getSchemaAsOf(db, txn, function (err, schema) {
        if (err) return callback(err)
        callback(null, mkFB(db, txn, schema))
      })
    } else {
      callback(null, mkFB(db, txn, fb.schema))
    }
  })
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

function getSchemaAsOf (db, txn, callback) {
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

    var schema = {
      byId: {},
      byAttr: {}
    }
    eachSeries(attrIds, function (id, next) {
      getEntity(db, txn, id, function (err, entity) {
        if (err) return next(err)

        entity.$e = id
        Object.freeze(entity)

        schema.byId[id] = entity
        schema.byAttr[entity['_s/attr']] = entity

        next()
      })
    }, function (err) {
      Object.freeze(schema.byId)
      Object.freeze(schema.byAttr)
      Object.freeze(schema)
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
    getSchemaAsOf(db, txn, function (err, schema) {
      if (err) return callback(err)
      callback(null, mkFB(db, txn, schema))
    })
  })
}

module.exports = function Transactor (db, initSchema, nextId) {
  nextId = nextId || cuid

  var currFB

  var transactQ = fastq(function (entities, callback) {
    try {
      transact(db, currFB, entities, nextId, function (err, fb) {
        if (err) return callback(err)
        if (fb) {
          currFB = fb
        }
        callback(null, currFB)
      })
    } catch (err) {
      callback(err)
    }
  })

  function syncSchema (callback) {
    var currSchema = currFB.schema.byAttr
    var goalSchema = Object.assign({}, systemSchema)

    if (initSchema) {
      Object.keys(initSchema).forEach(function (k) {
        goalSchema[k] = {}
        Object.keys(initSchema[k]).forEach(function (prop) {
          goalSchema[k]['_s/' + prop] = initSchema[k][prop]
        })
      })
    }

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

    Object.keys(goalSchema).forEach(function (attr) {
      var entity = {
        $e: (currSchema[attr] && currSchema[attr].$e) || nextId(),
        '_s/attr': attr
      }
      var foundDiff = !currSchema[attr]
      Object.keys(goalSchema[attr]).forEach(function (prop) {
        if (prop === '_s/attr') return
        if (prop === '$e') return
        var goalV = goalSchema[attr][prop]
        var currV = currSchema[attr] && currSchema[attr][prop]
        if (goalV !== currV) {
          entity[prop] = goalSchema[attr][prop]
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
    asOf: function (txn, callback) {
      callback = callback || promisify()
      onLoad(function (err) {
        if (err) return callback(err)
        getSchemaAsOf(db, txn, function (err, schema) {
          if (err) return callback(err)
          callback(null, mkFB(db, txn, schema))
        })
      })
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
