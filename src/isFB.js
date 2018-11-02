module.exports = function isFb (fb) {
  return Object.keys(fb).join(',') === 'db,txn,schema,q,get'
}
