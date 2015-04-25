var _ = require('lodash');

module.exports = function(db, config){

  var types = {//TODO make this extendable by config
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
    "Entity_ID": {
      validate: _.isString,
      encode: _.identity,
      decode: _.identity
    }
  };

  var schema = {
    "_db/type": {
      "_db/type": "String"
    },
    "_db/attribute": {
      "_db/type": "String"
    },
    "_db/txn-time": {
      "_db/type": "Date"
    }
  };

  //TODO auto-update the schema when new facts are asserted i.e. observe: aevto!_db/attribute

  return {
    schema: schema,//TODO make this hidden once this auto-updates from the db

    types: types,
    getTypeForAttribute: function(a, callback){
      //TODO implement the right way

      if(!types["String"].validate(a) || !schema.hasOwnProperty(a)){
        return callback(new Error("Attribute not found in schema: " + a));
      }
      callback(null, types[schema[a]["_db/type"]]);
    },
    getIndexesForAttribute: function(a, callback){
      //TODO implement the right way
      callback(null, ['eavto', 'aevto']);
    }
  };
};
