var _ = require('lodash');
var strHash = require('./utils/stringHash');
var NotFoundError = require('level-errors').NotFoundError;
var toPaddedBase36 = require('./utils/toPaddedBase36');

module.exports = function(db, options){
  options = options || {};
  var hashFn = _.isFunction(options.hashFn) ? options.hashFn : strHash;
  var hash_seq_length = _.isNumber(options.hash_seq_length) && options.hash_seq_length > 0 ? options.hash_seq_length : 2;
  var index_prefix = _.isString(options.index_prefix) ? options.index_prefix : 'hash!';


  var runtime_cache_collisions = {};

  var loadFromCache = function(val_hash, val, callback){
    if(!runtime_cache_collisions[val_hash].hasOwnProperty(val)){
      return false;//not in the cache
    }
    var o = runtime_cache_collisions[val_hash][val];
    if(o.is_new){
      //let's see if it still "is_new"
      db.get(o.key, function(err){
        if(err){
          if(err.type === 'NotFoundError'){
            return callback(null, o);//must still be new
          }else{
            return callback(err);
          }
        }
        runtime_cache_collisions[val_hash][val] = {hash: o.hash};//no longer new
        callback(null, runtime_cache_collisions[val_hash][val]);
      });
    }else{
      callback(null, o);
    }
    return true;
  };

  var put = function(val, callback){
    var val_hash = hashFn(val);

    if(runtime_cache_collisions.hasOwnProperty(val_hash)){
      if(loadFromCache(val_hash, val, callback)){
        return;
      }
    }else{
      runtime_cache_collisions[val_hash] = {};
    }

    var the_hash = null;
    db.createReadStream({
      keys: true,
      values: true,
      gte: index_prefix + val_hash + '\x00',
      lte: index_prefix + val_hash + '\xFF',
    }).on('data', function(data){
      var hash = data.key.substring(index_prefix.length);
      runtime_cache_collisions[val_hash][data.value] = {hash: hash};
      if(data.value === val){
        the_hash = hash;
      }
    }).on('error', function(err){
      callback(err);
    }).on('end', function(){
      //by the time this ends, some one else may have hashed the same value, so let's check the cache
      if(loadFromCache(val_hash, val, callback)){
        return;
      }
      if(the_hash !== null){
        callback(null, {hash: the_hash});
      }else{
        var seq_nums = _.map(runtime_cache_collisions[val_hash], function(o){
          var hash = o.hash;
          return parseInt(hash.substring(hash.length - hash_seq_length), 36);
        });
        var next_num = _.isEmpty(seq_nums) ? 0 : _.max(seq_nums) + 1;
        var hash = val_hash + toPaddedBase36(next_num, hash_seq_length);
        runtime_cache_collisions[val_hash][val] = {is_new: true, hash: hash, key: index_prefix + hash};
        callback(null, runtime_cache_collisions[val_hash][val]);
      }
    });
  };
  return {
    getHash: function(val, callback){
      put(val, function(err, d){
        if(err){
          callback(err);
        }else if(d.is_new){
          callback(new NotFoundError("No hash exists for that value"));
        }else{
          callback(null, d.hash);
        }
      });
    },
    put: put,
    putAndWrite: function(val, callback){
      put(val, function(err, d){
        if(err){
          callback(err);
        }else if(d.is_new){
          db.put(index_prefix + d.hash, val, function(err){
            callback(err, d.hash);
          });
        }else{
          callback(null, d.hash);
        }
      });
    },
    get: function(key, callback){
      db.get(index_prefix + key, callback);
    }
  };
};
