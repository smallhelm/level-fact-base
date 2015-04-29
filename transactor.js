var _ = require('lodash');
var λ = require('contra');
var inq = require('./inquisitor');
var SchemaUtils = require('./schema-utils');
var HashIndex = require('level-hash-index');
var Connection = require('./connection');
var toPaddedBase36 = require('./utils/toPaddedBase36');

var tupleToDBOps = function(fb, txn, tuple, callback){
  λ.map([tuple[0], tuple[1], tuple[2]], fb.hindex.put, function(err, hash_datas){
    if(err) return callback(err);

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

    SchemaUtils.getIndexesForAttribute(fb, tuple[1], function(err, indexes){
      if(err) return callback(err);

      indexes.forEach(function(index){
        ops.push({type: 'put', key: index + '!' + index.split('').map(function(k){
          return fact[k];
        }).join('!'), value: 0});
      });
      callback(null, ops);
    });
  });
};

var validateAndEncodeFactTuple = function(fb, fact_tuple, callback){
  if(!_.isArray(fact_tuple) || fact_tuple.length < 3 || fact_tuple.length > 4){//eavo
    return callback(new Error("fact_tuple must be an array defining EAV or EAVO"));
  }

  //entity
  var e = fact_tuple[0];
  if(!fb.types["Entity_ID"].validate(e)){
    return callback(new Error("Not a valid entity id"));
  }
  e = fb.types["Entity_ID"].encode(e);

  //attribute
  var a = fact_tuple[1];
  SchemaUtils.getTypeForAttribute(fb, a, function(err, type){
    if(err) return callback(err);

    //value
    var v = fact_tuple[2];
    if(!type.validate(v)){
      return callback(new Error("Invalid value for attribute " + a));
    }
    v = type.encode(v);

    //op
    var o = fact_tuple[3] === false ? 0 : 1;//default to 1

    callback(null, [e, a, v, o]);
  });
};

var validateAndEncodeFactTuples = function(fb, fact_tuples, callback){
  λ.map(fact_tuples, function(tuple, cb){
    validateAndEncodeFactTuple(fb, tuple, cb);
  }, callback);
};

module.exports = function(db, options, onStartup){
  options = options || {};

  var hindex = HashIndex(db);

  Connection(db, {hindex: hindex}, function(err, conn){
    if(err) return onStartup(err);

    onStartup(null, {
      connection: conn,
      transact: function(fact_tuples, tx_data, callback){

        var fb = conn.snap();

        conn.update(fb.txn + 1);//TODO find a better way i.e. maybe a list of pending txns? OR who cares if it fails, so long as the number still is higher than the previous?
        var txn = toPaddedBase36(fb.txn + 1, 6);//for lexo-graphic sorting

        //store facts about the transaction
        tx_data["_db/txn-time"] = new Date();
        _.each(tx_data, function(val, attr){
          fact_tuples.push(["_txid" + txn, attr, val]);
        });

        validateAndEncodeFactTuples(fb, fact_tuples, function(err, fact_tuples){
          if(err) return callback(err);

          λ.map(fact_tuples, function(tuple, callback){
            tupleToDBOps(fb, txn, tuple, callback);
          }, function(err, ops){
            if(err) return callback(err);

            db.batch(_.flatten(ops), function(err){
              if(err) return callback(err);

              //
              //TODO undo this hack
              var schema = conn.snap().schema;
              //TODO undo this hack
              //
              var attr_ids_transacted = _.pluck(fact_tuples.filter(function(fact){
                return fact[1] === '_db/attribute';
              }), 0);
              var fb = conn.snap();
              λ.map(attr_ids_transacted, function(id, callback){
                inq.getEntity(fb, id, callback);
              }, function(err, entities){
                if(err) return callback(err);

                entities.forEach(function(entity){
                  //
                  //TODO undo this hack
                  schema[entity["_db/attribute"]] = entity;
                  //TODO undo this hack
                  //
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
