var isFB = require('./isFB')
var promisify = require('./promisify')

module.exports = function get (fb, id, callback) {
  if (isFB(this)) {
    callback = id
    id = fb
    fb = this
  }

  callback = callback || promisify()

  fb.q([['?e', '?a', '?v']], {e: id}, [], function (err, results) {
    if (err) return callback(err)

    var entity = {$e: id}
    results.forEach(function (result) {
      entity[result.a] = result.v
    })

    callback(null, entity)
  })

  return callback.promise
}
