var dbRange = require('./dbRange')
var eachSeries = require('async/eachSeries')
var isFB = require('./isFB')
var promisify = require('./promisify')
var schemaTypes = require('./schemaTypes')

function escapeVar (elm) {
  return typeof elm === 'string'
    ? elm
      .replace(/^\\/, '\\\\')
      .replace(/^\?/, '\\?')
    : elm
}

function isVar (val) {
  return /^\?/.test(val)
}
function isKnown (val) {
  return val != null && !isVar(val) && typeof val !== 'function'
}

function bindToTuple (tuple, binding) {
  return tuple.map(function (val) {
    if (isVar(val)) {
      var key = val.substr(1)
      if (binding.hasOwnProperty(key)) {
        return escapeVar(binding[key])
      }
    }
    return val
  })
}

var selectIndex = (function () {
  var getKnowns = function (qFact) {
    var knowns = ''
    'eavt'.split('').forEach(function (key) {
      knowns += isKnown(qFact[key])
        ? key
        : '_'
    })
    return knowns
  }
  var mapping = {
    ____: 'eavto',

    e___: 'eavto',
    ea__: 'eavto',
    e_v_: 'eavto',
    eav_: 'eavto',

    _a__: 'aveto',
    _av_: 'aveto',

    __v_: 'vaeto',

    ___t: 'teavo',
    e__t: 'teavo',
    ea_t: 'teavo',
    e_vt: 'teavo',
    eavt: 'teavo',
    _a_t: 'teavo',
    _avt: 'teavo',
    __vt: 'teavo'
  }
  return function (qFact) {
    return mapping[getKnowns(qFact)]
  }
}())

var indexScore = {
  eavto: 300,
  teavo: 200,
  aveto: 100,
  vaeto: 0
}

function parseTuple (fb, tupleOrig, binding) {
  var tuple = bindToTuple(tupleOrig, binding)

  var qFact = {}
  qFact.e = tuple[0]
  qFact.a = tuple[1]
  qFact.v = tuple[2]
  qFact.t = tuple[3]

  if (isKnown(qFact.a) && !fb.schema.byAttr[qFact.a]) {
    throw new Error('Attribute `' + qFact.a + '` schema not found')
  }

  var index = selectIndex(qFact)
  var prefix = [index]
  var i
  for (i = 0; i < index.length; i++) {
    if (isKnown(qFact[index[i]])) {
      prefix.push(qFact[index[i]])
    } else {
      break
    }
  }

  var toBind = {}
  for (i = 0; i < index.length; i++) {
    if (isVar(qFact[index[i]])) {
      toBind[i + 1] = qFact[index[i]].substr(1)
    }
  }

  var filter
  var boundAttr = tupleOrig[2].substr(1)
  if (typeof binding[boundAttr] === 'function') {
    filter = binding[boundAttr]
    toBind[index.indexOf('v') + 1] = boundAttr
  }

  var score = indexScore[index] + (prefix.length * 10)
  if (isKnown(qFact.a)) {
    var type = fb.schema.byAttr[qFact.a]['_s/type']
    if (type === 'EntityID') {
      score += 1
    }
  }

  return {
    score: score,
    index: index,
    prefix: prefix,
    filter: filter,
    toBind: toBind
  }
}

function qTuple (fb, tuple, binding, callback) {
  var pt = parseTuple(fb, tuple, binding)

  var iE = pt.index.indexOf('e') + 1
  var iA = pt.index.indexOf('a') + 1
  var iV = pt.index.indexOf('v') + 1
  var iT = pt.index.indexOf('t') + 1

  var latestResults = {}

  dbRange(fb.db, {
    prefix: pt.prefix
  }, function (data) {
    var $t = data.key[iT]
    if ($t > fb.txn) {
      return
    }

    if (pt.filter && !pt.filter(data.key[iV])) {
      return
    }

    var resultKey = data.key[iE] + '|' + data.key[iA]
    if (latestResults[resultKey] && $t < latestResults[resultKey].t) {
      return
    }

    var result = {}
    Object.keys(pt.toBind).forEach(function (i) {
      result[pt.toBind[i]] = data.key[i]
    })

    latestResults[resultKey] = { t: $t, d: result }
  }, function (err) {
    if (err) return callback(err)

    var results = Object.keys(latestResults).map(function (key) {
      return Object.assign({}, binding, latestResults[key].d)
    })
    callback(null, results)
  })
}

function ResultSet () {
  var set = {}
  var arr = []
  return {
    add: function (binding) {
      var key = ''
      Object.keys(binding)
        .sort()
        .forEach(function (k) {
          key += k + ':' + binding[k] + ','
        })
      if (!set[key]) {
        arr.push(binding)
        set[key] = true
      }
    },
    toArray: function () {
      return arr
    }
  }
}

module.exports = function q (fb, tuples, binding, select, callback) {
  if (isFB(this)) {
    callback = select
    select = binding
    binding = tuples
    tuples = fb
    fb = this
  }

  binding = binding || {}
  callback = callback || promisify()

  Object.keys(binding).forEach(function (key) {
    var value = binding[key]
    if (schemaTypes.Date.validate(value)) {
      binding[key] = schemaTypes.Date.encode(value)
    }
  })

  tuples.sort(function (a, b) {
    a = parseTuple(fb, a, binding).score
    b = parseTuple(fb, b, binding).score
    return b - a
  })

  var memo = [binding]

  eachSeries(tuples, function (tuple, callback) {
    var rset = ResultSet()
    eachSeries(memo, function (binding, callback) {
      qTuple(fb, tuple, binding, function (err, results) {
        if (err) return callback(err)
        results.forEach(function (result) {
          rset.add(result)
        })
        callback()
      })
    }, function (err) {
      if (err) return callback(err)
      memo = rset.toArray()
      callback()
    })
  }, function (err) {
    if (err) return callback(err)

    if (select && select.length > 0) {
      var rset = ResultSet()
      memo.forEach(function (binding) {
        var r = {}
        select.forEach(function (key) {
          r[key] = binding[key]
        })
        rset.add(r)
      })
      callback(null, rset.toArray())
      return
    }

    callback(null, memo)
  })

  return callback.promise
}
