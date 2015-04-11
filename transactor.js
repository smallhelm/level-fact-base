var _ = require('lodash');
var async = require('async');
var HashIndex = require('./level-hash-index');
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

module.exports = function(db, options, onStartup){
  options = options || {};

  var hindex = HashIndex(db);
  var inq = Inquisitor(db, {HashIndex: hindex});

  //warm up the transactor by loading in it's current state
  async.parallel({
    transaction_n: function(callback){
      inq.q([["?e", "?a", "?v", "?txn"]], [{}], function(err, results){
        if(err){
          return callback(err);
        }
        var txns = _.pluck(results, "?txn");
        callback(null, txns.length === 0 ? 0 : _.max(txns));
      });
    },
    schema: function(callback){
      callback(null, {});//TODO read user entered schema to extend the native db schema
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
        tx_data["_db/time"] = new Date().toISOString();
        _.each(tx_data, function(val, attr){
          fact_tuples.push(["_txid" + txn, attr, val]);
        });

        //TODO validate and encode fact_tuples
        //       + attributes must exist in the schema
        //       + use schema to validate values
        //       + use schema to encode values
        //       + assert e,a,v are all strings in the end

        async.map(fact_tuples, function(tuple, callback){
          tupleToDBOps(hindex, txn, tuple, callback);  
        }, function(err, ops){
          if(err) callback(err);
          else db.batch(_.flatten(ops), callback);
        });
      }
    });
  });
};
