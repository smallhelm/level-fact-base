var _ = require('lodash')
var λ = require('contra')
var dbRange = require('./dbRange')
var promisify = require('./promisify')
var isFB = require('./isFB')

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
  return val != null && !isVar(val)
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

function qTuple (fb, tupleOrig, binding, callback) {
  var tuple = bindToTuple(tupleOrig, binding)

  var qFact = {}
  qFact.e = tuple[0]
  qFact.a = tuple[1]
  qFact.v = tuple[2]
  qFact.t = tuple[3]

  var index = selectIndex(qFact)
  var prefix = [index]
  for (var i = 0; i < index.length; i++) {
    if (isKnown(qFact[index[i]])) {
      prefix.push(qFact[index[i]])
    } else {
      break
    }
  }

  var toBind = {}
  for (var i = 0; i < index.length; i++) {
    if (isVar(qFact[index[i]])) {
      toBind[i + 1] = qFact[index[i]].substr(1)
    }
  }

  var iE = index.indexOf('e') + 1
  var iA = index.indexOf('a') + 1
  var iT = index.indexOf('t') + 1

  var latestResults = {}

  dbRange(fb.db, {
    prefix: prefix
  }, function (data) {
    var $t = data.key[iT]
    if ($t > fb.txn) {
      return
    }
    var resultKey = data.key[iE] + '|' + data.key[iA]
    if (latestResults[resultKey] && $t < latestResults[resultKey].t) {
      return
    }

    var result = {}
    Object.keys(toBind).forEach(function (i) {
      result[toBind[i]] = data.key[i]
    })

    latestResults[resultKey] = {t: $t, d: result}
  }, function (err) {
    if (err) return callback(err)

    var results = Object.keys(latestResults).map(function (key) {
      return Object.assign({}, latestResults[key].d, binding)
    })
    callback(null, results)
  })
}

module.exports = function q (fb, tuples, binding, questions, callback) {
  if (isFB(this)) {
    callback = questions
    questions = binding
    binding = tuples
    tuples = fb
    fb = this
  }

  binding = binding || {}
  callback = callback || promisify()

  var memo = [binding]
  λ.each.series(tuples, function (tuple, callback) {
    λ.map.series(memo, function (binding, callback) {
      qTuple(fb, tuple, binding, function (err, results) {
        if (err) return callback(err)
        callback(null, results)
      })
    }, function (err, nextBindings) {
      if (err) return callback(err)
      memo = _.flatten(nextBindings)
      memo = _.uniqBy(memo, function (binding) {
        return JSON.stringify(binding)
      })
      callback()
    })
  }, function (err) {
    if (err) return callback(err)

    if (questions && questions.length > 0) {
      memo = _.map(memo, function (binding) {
        var r = {}
        _.each(questions, function (key) {
          r[key] = binding[key]
        })
        return r
      })
    }

    memo = _.uniqBy(memo, function (binding) {
      return JSON.stringify(binding)
    })

    callback(null, memo)
  })

  return callback.promise
}
