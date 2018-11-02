var isFB = require('./isFB')
var promisify = require('./promisify')

module.exports = function get (fb, $e, callback) {
  if (isFB(this)) {
    callback = $e
    $e = fb
    fb = this
  }

  callback = callback || promisify()

  fb.q([['?e', '?a', '?v']], { e: $e }, [], function (err, results) {
    if (err) return callback(err)

    var entity = { $e: $e }
    results.forEach(function (result) {
      entity[result.a] = result.v
    })

    callback(null, entity)
  })

  return callback.promise
}
