var _ = require('lodash');

var db_types = {
  "Date": {
    validate: _.isDate,
    encode: function(d){
      return d.toISOString();
    },
    decode: function(s){
      return new Date(s);
    }
  },
  "String": {
    validate: _.isString,
    encode: _.identity,
    decode: _.identity
  },
  "Integer": {
    validate: function(n){
      return _.isNumber(n) && (n % 1 === 0);
    },
    encode: function(n){
      return n.toString();
    },
    decode: function(s){
      return parseInt(s, 10) || 0;
    }
  },
  "Number": {
    validate: _.isNumber,
    encode: function(n){
      return n.toString();
    },
    decode: function(s){
      return parseFloat(s);
    }
  },
  "Boolean": {
    validate: function(v){
      return v === true || v === false;
    },
    encode: function(v){
      return v ? '1' : '0';
    },
    decode: function(s){
      return s === '0' ? false : true;
    }
  },
  "Entity_ID": {
    validate: _.isString,
    encode: _.identity,
    decode: _.identity
  }
};

var db_schema = [
  {
    "_db/attribute": "_db/attribute",
    "_db/type": "String"
  },
  {
    "_db/attribute": "_db/type",
    "_db/type": "String"
  },
  {
    "_db/attribute": "_db/is-multi-valued",
    "_db/type": "Boolean"
  },
  {
    "_db/attribute": "_db/txn-time",
    "_db/type": "Date"
  }
];

module.exports = {
  db_types: db_types,
  db_schema: db_schema,
  index_names: [
    'eavto',
    'aveto',
    'vaeto',
    'teavo'
  ]
};
