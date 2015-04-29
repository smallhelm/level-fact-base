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

var loadUserSchema = function(fb, callback){
  inq.q(fb, [["?attr_id", "_db/attribute"]], [{}], function(err, results){
    if(err) return callback(err);

    λ.map(results, function(result, callback){
      inq.getEntity(fb, result["?attr_id"], callback);
    }, function(err, entities){
      if(err) return callback(err);

      var schema = {};
      entities.forEach(function(entity){
        schema[entity["_db/attribute"]] = entity;
      });
      callback(null, schema);
    });
  });
};

module.exports = function(db, options, callback){
  options = options || {};

  var hindex = options.hindex || HashIndex(db);

  getLatestedTxn(db, function(err, latest_transaction_n){
    if(err) return callback(err);

    loadUserSchema({
      txn: latest_transaction_n,
      schema: db_schema,

      db: db,
      hindex: hindex
    }, function(err, user_schema){
      if(err) return callback(err);

      var schema = _.assign({}, user_schema, db_schema);

      callback(null, {
        update: function(t){
          latest_transaction_n = t;
        },
        snap: function(){
          return {
            txn: latest_transaction_n,
            types: db_types,
            schema: schema,

            db: db,
            hindex: hindex
          };
        }
      });
    });
  });
};
