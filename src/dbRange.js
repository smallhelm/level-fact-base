var promisify = require('./promisify')

module.exports = function dbRange (db, opts, onData, callbackOrig) {
  if (!callbackOrig) {
    callbackOrig = promisify()
  }

  var hasCalledback = false
  function callback (err) {
    if (hasCalledback) return
    hasCalledback = true
    callbackOrig(err)
  }

  if (opts.prefix) {
    opts.gte = opts.prefix
    opts.lte = opts.prefix.concat([void 0])
  }

  var stream = db.createReadStream(opts)
  stream.on('error', callback)
  stream.on('end', callback)
  function stopRange () {
    stream.destroy()
    callback()
  }
  stream.on('data', function (data) {
    onData(data, stopRange)
  })

  return callbackOrig.promise
}
