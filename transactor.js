var _ = require('lodash');
var async = require('async');
var HashIndex = require('level-hash-index');
var Inquisitor = require('./inquisitor');
var toPaddedBase36 = require('./utils/toPaddedBase36');

var tupleToDBOps = function(hindex, txn, tuple, callback){
  async.map([tuple[0], tuple[1], tuple[2]], hindex.put, function(err, hash_datas){
    if(err){
      callback(err);
      return;
    }
    var ops = [];
    var fact = {
      t: txn,
      o: tuple[3] === false ? 0 : 1//default to 1
    };
    'eav'.split('').forEach(function(k, i){
      fact[k] = hash_datas[i].hash;
      if(hash_datas[i].is_new){
        ops.push({type: 'put', key: hash_datas[i].key, value: tuple[i]});
      }
    });

    var indexes = ['eavto', 'aevto'];//TODO decide this based on attribute schema
    indexes.forEach(function(index){
      ops.push({type: 'put', key: index + '!' + index.split('').map(function(k){
        return fact[k];
      }).join('!'), value: 0});
    });
    callback(null, ops);
  });
};

var DB_TYPES = {
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

var validateAndEncodeFactTuple = function(fact_tuple, schema, callback){
  if(!_.isArray(fact_tuple) || fact_tuple.length < 3 || fact_tuple.length > 4){//eavo
    return callback(new Error("fact_tuple must be an array defining EAV or EAVO"));
  }

  //entity
  var e = fact_tuple[0];
  if(!DB_TYPES["Entity_ID"].validate(e)){
    return callback(new Error("Not a valid entity id"));
  }
  e = DB_TYPES["Entity_ID"].encode(e);

  //attribute
  var a = fact_tuple[1];
  if(!DB_TYPES["String"].validate(a) || !schema.hasOwnProperty(a)){
    return callback(new Error("Attribute not found in schema: " + a));
  }

  //value
  var v = fact_tuple[2];
  var type = DB_TYPES[schema[a]["_db/type"]];
  if(!type.validate(v)){
    return callback(new Error("Invalid value for attribute " + a));
  }
  v = type.encode(v);

  //op
  var o = fact_tuple[3] === false ? 0 : 1;//default to 1

  callback(null, [e, a, v, o]);
};

var validateAndEncodeFactTuples = function(fact_tuples, schema, callback){
  async.map(fact_tuples, function(tuple, cb){
    validateAndEncodeFactTuple(tuple, schema, cb);
  }, callback);
};

module.exports = function(db, options, onStartup){
  options = options || {};

  var hindex = HashIndex(db);
  var inq = Inquisitor(db, {HashIndex: hindex});

  //warm up the transactor by loading in it's current state
  async.parallel({
    transaction_n: function(callback){
      inq.q([[null, "_db/txn-time", null, "?txn"]], [{}], function(err, results){
        if(err){
          return callback(err);
        }
        var txns = _.pluck(results, "?txn");
        callback(null, txns.length === 0 ? 0 : _.max(txns));
      });
    },
    schema: function(callback){
      inq.q([["?attr_id", "_db/attribute"]], [{}], function(err, results){
        if(err){
          return callback(err);
        }
        var schema = {
          "_db/type": {
            "_db/type": "String"
          },
          "_db/attribute": {
            "_db/type": "String"
          },
          "_db/txn-time": {
            "_db/type": "Date"
          },

          //TODO remove the rest (currently stubbed in for now for tests)
          "is": {"_db/type": "String"},
          "email": {"_db/type": "String"},
          "father": {"_db/type": "String"},
          "mother": {"_db/type": "String"},
          "name": {"_db/type": "String"},
          "age": {"_db/type": "String"},
          "user_id": {"_db/type": "String"}
        };
        async.map(_.pluck(results, "?attr_id"), inq.getEntity, function(err, entities){
          if(err){
            return callback(err);
          }
          entities.forEach(function(entity){
            schema[entity["_db/attribute"]] = entity;
          });
          callback(null, schema);
        });
      });
    }
  }, function(err, transactor_state){
    if(err){
      return onStartup(err);
    }
    var schema = transactor_state.schema;
    var transaction_n = transactor_state.transaction_n;

    onStartup(null, {
      transact: function(fact_tuples, tx_data, callback){

        transaction_n++;//TODO find a better way i.e. maybe a list of pending txns? OR who cares if it fails, so long as the number still is higher than the previous?
        var txn = toPaddedBase36(transaction_n, 6);//for lexo-graphic sorting

        //store facts about the transaction
        tx_data["_db/txn-time"] = new Date();
        _.each(tx_data, function(val, attr){
          fact_tuples.push(["_txid" + txn, attr, val]);
        });

        validateAndEncodeFactTuples(fact_tuples, schema, function(err, fact_tuples){
          if(err){
            return callback(err);
          }
          async.map(fact_tuples, function(tuple, callback){
            tupleToDBOps(hindex, txn, tuple, callback);
          }, function(err, ops){
            if(err) callback(err);
            else db.batch(_.flatten(ops), function(err){
              if(err) callback(err);

              //TODO
              //TODO more optimal way of updating the schema
              //TODO
              var attr_ids_transacted = _.pluck(fact_tuples.filter(function(fact){
                return fact[1] === '_db/attribute';
              }), 0);
              async.map(attr_ids_transacted, inq.getEntity, function(err, entities){
                if(err) callback(err);

                entities.forEach(function(entity){
                  schema[entity["_db/attribute"]] = entity;
                });
                callback();
              });
            });
          });
        });
      }
    });
  });
};
