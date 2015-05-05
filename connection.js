var _ = require('lodash');
var λ = require('contra');
var inq = require('./inquisitor');
var HashIndex = require('level-hash-index');

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

var db_schema = {
  "_db/type": {
    "_db/type": "String"
  },
  "_db/attribute": {
    "_db/type": "String"
  },
  "_db/is-multi-valued": {
    "_db/type": "Boolean"
  },
  "_db/txn-time": {
    "_db/type": "Date"
  }
};

var getLatestedTxn = function(db, callback){
  var stream = db.createReadStream({
    keys: true,
    values: false,
    reverse: true,
    gte: 'teavo!\x00',
    lte: 'teavo!\xFF',
  }).on('data', function(data){
    var txn = parseInt(data.split('!')[1], 36);
    callback(null, txn);
    stream.destroy();
  }).on('error', function(err){
    callback(err);
  }).on('end', function(){
    callback(null, 0);
  });
};

var loadSchemaFromIds = function(fb, ids, callback){
  λ.map(ids, function(id, callback){
    inq.getEntity(fb, id, callback);
  }, function(err, entities){
    if(err) return callback(err);

    var schema = {};
    schema["_db/attribute-hashes"] = {};

    λ.each(entities, function(entity, callback){
      if(!_.has(entity, "_db/attribute")){
        return callback(null);//just ignore it
      }
      var a = entity["_db/attribute"];
      schema[a] = entity;

      fb.hindex.getHash(a, function(err, hash){
        if(err) return callback(err);

        schema[a]["_db/attribute-hash"] = hash
        schema["_db/attribute-hashes"][hash] = a;

        callback(null);//done with this entity
      });
    }, function(err){
      callback(err, schema);
    });
  });
};

var loadUserSchema = function(fb, callback){
  inq.q(fb, [["?attr_id", "_db/attribute"]], [{}], function(err, results){
    if(err) return callback(err);

    loadSchemaFromIds(fb, results.map(function(result){
      return result["?attr_id"];
    }), callback);
  });
};

var loadTheBaseSchema = function(hindex, callback){
  var schema = {};
  schema["_db/attribute-hashes"] = {};

  λ.each(Object.keys(db_schema), function(a, done){
    schema[a] = _.cloneDeep(db_schema[a]);
    schema[a]["_db/attribute"] = a;

    hindex.put(a, function(err, h){
      if(err) return done(err);
      schema[a]["_db/attribute-hash"] = h.hash;
      schema["_db/attribute-hashes"][h.hash] = a;
      done(null);
    });
  }, function(err){
    if(err) return callback(err);
    callback(null, schema);
  });
};

module.exports = function(db, options, callback){
  options = options || {};

  var hindex = options.hindex || HashIndex(db);

  var makeFB = function(txn, schema){
    return {
      txn: txn,
      types: db_types,
      schema: schema,

      db: db,
      hindex: hindex
    };
  };

  loadTheBaseSchema(hindex, function(err, base_schema){
    if(err) return callback(err);

    var loadSchemaAsOf = function(txn, callback){
      loadUserSchema(makeFB(txn, base_schema), function(err, user_schema){
        if(err) return callback(err);
        callback(null, _.assign({}, user_schema, base_schema));
      });
    };

    getLatestedTxn(db, function(err, latest_transaction_n){
      if(err) return callback(err);

      loadSchemaAsOf(latest_transaction_n, function(err, latest_schema){
        if(err) return callback(err);

        callback(null, {
          update: function(new_txn, schema_changes){
            latest_transaction_n = new_txn;
            latest_schema = _.assign({}, latest_schema, schema_changes);
          },
          snap: function(){
            return makeFB(latest_transaction_n, latest_schema);
          },
          asOf: function(txn, callback){
            loadSchemaAsOf(txn, function(err, schema){
              if(err) return callback(err);
              callback(null, makeFB(txn, schema));
            });
          },
          loadSchemaFromIds: function(txn, ids, callback){
            loadSchemaFromIds(makeFB(txn, base_schema), ids, callback);
          }
        });
      });
    });
  });
};
