module.exports = function () {
  var callback
  var promise = new Promise(function (resolve, reject) {
    callback = function (err, value) {
      if (err) reject(err)
      else resolve(value)
    }
  })
  callback.promise = promise
  return callback
}
