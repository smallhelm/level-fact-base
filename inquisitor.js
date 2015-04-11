var _ = require('lodash');
var async = require('async');
var HashIndex = require('./level-hash-index');
var toPaddedBase36 = require('./utils/toPaddedBase36');

var escapeRegExp = function(str){
  return str.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
};

var bindToTuple = function(tuple, binding){
  return tuple.map(function(e){
    if(binding.hasOwnProperty(e)){
      return binding[e];
    }
    return e;
  });
};

var isVar = function(elm){
  return _.isString(elm) && elm[0] === '?';
};

var parseElement = function(hindex, elm, callback){
  if(isVar(elm)){
    return callback(null, {var_name: elm});
  }else if(_.isString(elm)){
    hindex.getHash(elm, function(err, hash){
      if(err){
        return callback(err);
      }
      callback(null, {value: elm, hash: hash});
    });
  }else{
    callback(null, {is_blank: true});
  }
};

var parseTuple = function(hindex, tuple, callback){
  async.parallel({
    e: async.apply(parseElement, hindex, tuple[0]),
    a: async.apply(parseElement, hindex, tuple[1]),
    v: async.apply(parseElement, hindex, tuple[2]),
    t: function(callback){
      if(isVar(tuple[3])){
        return callback(null, {var_name: tuple[3]});
      }else if(_.isNumber(tuple[3])){
        var txn = toPaddedBase36(tuple[3], 6);
        return callback(null, {value: txn, hash: txn});
      }
      callback(null, {is_blank: true});
    },
    o: function(callback){
      if(isVar(tuple[4])){
        return callback(null, {var_name: tuple[4]});
      }else if(tuple[4] === true || tuple[4] === false){
        return callback(null, {value: tuple[4], hash: tuple[4]});
      }
      callback(null, {is_blank: true});
    }
  }, callback);
};

var selectIndex = (function(){
  var getKnowns = function(q_fact){
    var knowns = [];
    "eav".split("").forEach(function(key){
      if(q_fact[key].hasOwnProperty("hash")){
        knowns.push(key);
      }
    });
    return knowns.sort().join("");
  };
  var mapping = {
    '': 'eavto',
    'e': 'eavto',
    'a': 'aevto',
    'v': 'vaeto',
    'av': 'aveto',
    'ev': 'eavto',
    'ae': 'eavto',
    'aev': 'eavto',
  };
  return function(q_fact){
    return 'eavto';//TODO select the index based on attribute schema
    return mapping[getKnowns(q_fact)];
  };
}());

var toMatcher = function(index_to_use, q_fact){

  var prefix = index_to_use + '!';
  var prefix_parts = [];
  var found_a_gap = false;

  var regex = escapeRegExp(prefix) + index_to_use.split("").map(function(k){
    if(q_fact[k].hasOwnProperty('hash')){
      if(!found_a_gap){
        prefix_parts.push(q_fact[k].hash);
      }
      return escapeRegExp(q_fact[k].hash);
    }else{
      found_a_gap = true;
      return '.*';
    }
  }).join(escapeRegExp('!'));

  return {
    prefix: prefix + prefix_parts.join('!'),
    matchRegExp: new RegExp(regex)
  };
}; 

var findMatchingKeys = function(db, matcher, callback){
  var results = [];
  db.createReadStream({
    keys: true,
    values: false,
    gte: matcher.prefix + '\x00',
    lte: matcher.prefix + '\xFF',
  }).on('data', function(data){
    if(matcher.matchRegExp.test(data)){
      results.push(data);
    }
  }).on('error', function(err){
    callback(err);
  }).on('end', function(){
    callback(null, results);
  });
};

var keyToFact = function(key){
  var parts = key.split("!");
  var index_name = parts[0];
  var fact = {};
  index_name.split('').forEach(function(k, i){
    var part = parts[i + 1];
    if(k === 't'){
      fact[k] = {value: parseInt(part, 36)};
    }else if(k === 'o'){
      fact[k] = {value: part === '1'};
    }else{
      fact[k] = part;
    }
  });
  return fact;
};

var bindKeys = function(matching_keys, q_fact){
  var binding = {};//to ensure unique-ness

  var only_the_latest = q_fact.t.is_blank;//TODO also based on the cardiality of q_fact.a's schema
  var latest_for = {};//latest for the same e+a

  var var_names = "eavto".split('').filter(function(k){
    return q_fact[k].hasOwnProperty('var_name');
  }).map(function(k){
    return [q_fact[k].var_name, k];
  });

  matching_keys.forEach(function(key){
    var fact = keyToFact(key);

    if(only_the_latest){
      if(latest_for.hasOwnProperty(fact.e + fact.a)){
        if(latest_for[fact.e + fact.a] > fact.t.value){
          return;//not the latest, so skip the rest
        }
      }
      latest_for[fact.e + fact.a] = fact.t.value;
    }

    var vars = {};
    var hash_key = '';
    var_names.forEach(function(p){
      var k = p[1];
      vars[p[0]] = fact[k];
      hash_key += _.isString(fact[k]) ? fact[k] : fact[k].value;
    });
    binding[hash_key] = vars;
  });
  return _.values(binding);
};

var qTuple = function(db, hindex, tuple, orig_binding, callback){
  parseTuple(hindex, bindToTuple(tuple, orig_binding), function(err, q_fact){
    if(err){
      if(err.type === 'NotFoundError'){
        //one of the tuple values were not found in the hash, so there must be no results
        return callback(null, []);
      }
      return callback(err);
    }
    var index_to_use = selectIndex(q_fact);

    findMatchingKeys(db, toMatcher(index_to_use, q_fact), function(err, matching_keys){
      if(err){
        return callback(err);
      }
      var bindings = bindKeys(matching_keys, q_fact);

      //de-hash the bindings
      async.map(bindings, function(binding, callback){
        async.map(_.pairs(binding), function(p, callback){
          if(_.isString(p[1])){
            hindex.get(p[1], function(err, val){
              callback(err, [p[0], val]);
            });
          }else{
            callback(null, [p[0], p[1].value]);
          }
        }, function(err, pairs){
          callback(err, _.assign({}, orig_binding, _.object(pairs)));
        });
      }, callback);
    });
  });
};

module.exports = function(db, options){
  options = options || {};

  var hindex = options.HashIndex || HashIndex(db);
  var qTuple_bound = function(tuple, binding, callback){
    //TODO validate tuple
    qTuple(db, hindex, tuple, binding, callback);
  };
  var q = function(tuples, bindings, callback){
    //TODO validate tuples is an array
    async.reduce(tuples, bindings, function(bindings, tuple, callback){
      async.map(bindings, function(binding, callback){
        qTuple(db, hindex, tuple, binding, callback);
      }, function(err, bindings){
        if(err) callback(err);
        else callback(null, _.flatten(bindings));
      });
    }, callback);
  };
  return {
    qTuple: qTuple_bound,
    q: q,
    getEntity: function(e, callback){
      q([["?e", "?a", "?v"]], [{"?e": e}], function(err, results){
        if(err) return callback(err);
        var o = {};
        results.forEach(function(result){
          o[result["?a"]] = result["?v"];
          //TODO cardinality = multiple
        });
        callback(null, o);
      });
    }
  };
};
