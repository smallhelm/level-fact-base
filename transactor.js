var _ = require('lodash');
var λ = require('contra');
var AsyncQ = require('./any-value-async-q');
var SchemaUtils = require('./schema-utils');
var HashIndex = require('level-hash-index');
var Connection = require('./connection');
var constants = require('./constants');
var toPaddedBase36 = require('./utils/toPaddedBase36');

var tupleToDBOps = function(fb, txn, tuple, callback){
  λ.map([tuple[0], tuple[1], tuple[2]], fb.hindex.put, function(err, hash_datas){
    if(err) return callback(err);

    var ops = [];
    var fact = {
      t: toPaddedBase36(txn, 6),//for lexo-graphic sorting
      o: tuple[3]
    };
    'eav'.split('').forEach(function(k, i){
      fact[k] = hash_datas[i].hash;
      if(hash_datas[i].is_new){
        ops.push({type: 'put', key: hash_datas[i].key, value: tuple[i]});
      }
    });

    var indexes = constants.index_names;

    indexes.forEach(function(index){
      ops.push({type: 'put', key: index + '!' + index.split('').map(function(k){
        return fact[k];
      }).join('!'), value: 0});
    });
    callback(null, ops);
  });
};

var validateAndEncodeFactTuple_THIS_MAY_THROWUP = function(fb, fact_tuple){
  if(!_.isArray(fact_tuple) || fact_tuple.length < 3 || fact_tuple.length > 4){//eavo
    throw new Error("fact_tuple must be an array defining EAV or EAVO");
  }

  //entity
  var e = fact_tuple[0];
  if(!fb.types["Entity_ID"].validate(e)){
    throw new Error("Not a valid entity id");
  }
  e = fb.types["Entity_ID"].encode(e);

  //attribute
  var a = fact_tuple[1];
  var type = SchemaUtils.getTypeForAttribute_THIS_MAY_THROWUP(fb, a);

  //value
  var v = fact_tuple[2];
  if(!type.validate(v)){
    throw new Error("Invalid value for attribute " + a);
  }
  v = type.encode(v);

  //op
  var o = fact_tuple[3] === false ? 0 : 1;//default to 1

  return [e, a, v, o];
};

var validateAndEncodeFactTuples_THIS_MAY_THROWUP = function(fb, fact_tuples){
  return fact_tuples.map(function(tuple){
    return validateAndEncodeFactTuple_THIS_MAY_THROWUP(fb, tuple);
  });
};

var validateAndEncodeFactTuplesToDBOps = function(fb, txn, fact_tuples, callback){
  try{
    fact_tuples = validateAndEncodeFactTuples_THIS_MAY_THROWUP(fb, fact_tuples);
  }catch(err){
    return callback(err);
  }

  λ.map(fact_tuples, function(tuple, callback){
    tupleToDBOps(fb, txn, tuple, callback);
  }, function(err, ops_per_fact){
    callback(err, _.flatten(ops_per_fact));
  });
};

var factTuplesToSchemaChanges = function(conn, txn, fact_tuples, callback){
  var attr_ids = _.pluck(fact_tuples.filter(function(fact){
    return fact[1] === '_db/attribute';
  }), 0);

  if(attr_ids.length === 0){
    return callback(null, {});
  }
  conn.loadSchemaFromIds(txn, attr_ids, callback);
};

module.exports = function(db, options, onStartup){
  if(arguments.length === 2){
    onStartup = options;
    options = {};
  }
  options = options || {};

  var hindex = options.hindex || HashIndex(db);

  Connection(db, {hindex: hindex}, function(err, conn){
    if(err) return onStartup(err);

    var q = AsyncQ(function(data, callback){
      var fact_tuples = data[0];
      var tx_data = data[1];

      var fb = conn.snap();
      var txn = fb.txn + 1;

      //store facts about the transaction
      tx_data["_db/txn-time"] = new Date();
      _.each(tx_data, function(val, attr){
        fact_tuples.push(["_txid" + txn, attr, val]);
      });

      validateAndEncodeFactTuplesToDBOps(fb, txn, fact_tuples, function(err, ops){
        if(err) return callback(err);

        fb.db.batch(ops, function(err){
          if(err) return callback(err);

          factTuplesToSchemaChanges(conn, txn, fact_tuples, function(err, schema_changes){
            if(err) return callback(err);

            conn.update(txn, schema_changes);
            callback(null, conn.snap());
          });
        });
      });
    });

    onStartup(null, {
      connection: conn,
      transact: function(fact_tuples, tx_data, callback){
        if(arguments.length === 2){
          callback = tx_data;
          tx_data = {};
        }
        q.push([fact_tuples, tx_data], callback);
      }
    });
  });
};
