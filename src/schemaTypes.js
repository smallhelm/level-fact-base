module.exports = {
  'Date': {
    validate: function (v) {
      return Object.prototype.toString.call(v) === '[object Date]' && !Number.isNaN(v.getTime())
    },
    encode: function (v) {
      return v.toISOString()
    }
  },
  'String': {
    validate: function (v) {
      return typeof v === 'string'
    }
  },
  'Integer': {
    validate: function (v) {
      return typeof v === 'number' && Number.isFinite(v) && Number.isInteger(v)
    }
  },
  'Number': {
    validate: function (v) {
      return typeof v === 'number' && Number.isFinite(v)
    }
  },
  'Boolean': {
    validate: function (v) {
      return v === true || v === false
    }
  },
  'EntityID': {
    validate: function (v) {
      return typeof v === 'string'
    }
  }
}
