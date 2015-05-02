var _ = require('lodash');
var λ = require('contra');
var SchemaUtils = require('./schema-utils');
var escapeRegExp = require('escape-regexp');
var HashIndex = require('level-hash-index');
var toPaddedBase36 = require('./utils/toPaddedBase36');

var escapeVar = function(elm){
  return _.isString(elm) ? elm.replace(/^\\/, "\\\\").replace(/^\?/, "\\?") : elm;
};

var unEscapeVar = function(elm){
  return _.isString(elm) ? elm.replace(/^\\/, "") : elm;
};

var isVar = function(elm){
  return _.isString(elm) && elm[0] === '?';
};

var isTheThrowAwayVar = function(elm){
  return elm === '?_';
};

var bindToTuple = function(tuple, binding){
  return tuple.map(function(e){
    if(binding.hasOwnProperty(e)){
      return escapeVar(binding[e]);
    }
    return e;
  });
};

var parseElement = function(hindex, tuple, i, callback){
  var elm = tuple.length < i + 1 ? '?_' : tuple[i];
  if(isTheThrowAwayVar(elm)){
    callback(null, {is_blank: true});
  }else if(isVar(elm)){
    callback(null, {var_name: elm});
  }else if(i < 3 && _.isString(elm)){
    elm = unEscapeVar(elm);
    hindex.getHash(elm, function(err, hash){
      if(err) callback(err);
      else callback(null, {value: elm, hash: hash});
    });
  }else if(i === 3 && _.isNumber(elm)){
    var txn = toPaddedBase36(elm, 6);
    callback(null, {value: txn, hash: txn});
  }else if(i === 4 && (elm === true || elm === false)){
    callback(null, {value: elm, hash: elm});
  }else{
    callback(new Error('element ' + i + ' in tuple has invalid type'));
  }
};

var parseTuple = function(hindex, tuple, callback){
  λ.concurrent({
    e: λ.curry(parseElement, hindex, tuple, 0),
    a: λ.curry(parseElement, hindex, tuple, 1),
    v: λ.curry(parseElement, hindex, tuple, 2),
    t: λ.curry(parseElement, hindex, tuple, 3),
    o: λ.curry(parseElement, hindex, tuple, 4)
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

var parseKey = function(key){
  var parts = key.split("!");
  var index_name = parts[0];
  var hash_fact = {};
  index_name.split('').forEach(function(k, i){
    var part = parts[i + 1];
    if(k === 't'){
      hash_fact[k] = parseInt(part, 36);
    }else if(k === 'o'){
      hash_fact[k] = part === '1';
    }else{
      hash_fact[k] = part;
    }
  });
  return hash_fact;
};

var forEachMatchingHashFact = function(fb, matcher, iterator, done){
  fb.db.createReadStream({
    keys: true,
    values: false,
    gte: matcher.prefix + '\x00',
    lte: matcher.prefix + '\xFF',
  }).on('data', function(key){
    if(!matcher.matchRegExp.test(key)){
      return;
    }
    var hash_fact = parseKey(key);
    if(hash_fact.t > fb.txn){
      return;//this fact is too new, so ignore it
    }
    iterator(hash_fact);
  }).on('error', function(err){
    done(err);
  }).on('end', function(){
    done(null);
  });
};

var isMultiValued = function(fb, a){
  try{
    return SchemaUtils.isAttributeMultiValued_THIS_MAY_THROWUP(fb, a);
  }catch(e){
    return false;
  }
};

var SetOfBindings = function(var_names){
  var set = {};
  var latest_for = {};//latest for the same e+a
  var next_key = 0;
  return {
    add: function(only_the_latest, hash_fact){
      next_key++;
      var key_for_latest_for = only_the_latest ? hash_fact.e + hash_fact.a : next_key;

      if(latest_for.hasOwnProperty(key_for_latest_for)){
        if(latest_for[key_for_latest_for].txn > hash_fact.t){
          return;//not the latest, so skip the rest
        }
      }
      var binding = {};
      var hash_key = '';//to ensure uniqueness
      var_names.forEach(function(p){
        var k = p[1];
        binding[p[0]] = hash_fact[k];
        hash_key += hash_fact[k];
      });
      set[hash_key] = binding;
      latest_for[key_for_latest_for] = {txn: hash_fact.t, hash_key: hash_key};
    },
    toArray: function(){
      return _.unique(_.pluck(latest_for, 'hash_key')).map(function(key){
        return set[key];
      });
    }
  };
};

var bindHashFacts = function(fb, hash_facts, q_fact){
  var only_the_latest = q_fact.t.is_blank;
  if(isMultiValued(fb, q_fact.a.value)){
    only_the_latest = false;
  }

  var var_names = "eavto".split('').filter(function(k){
    return q_fact[k].hasOwnProperty('var_name');
  }).map(function(k){
    return [q_fact[k].var_name, k];
  });

  var s = SetOfBindings(var_names);
  hash_facts.forEach(function(hash_fact){
    s.add(only_the_latest, hash_fact);
  });
  return s.toArray();
};

var qTuple = function(fb, tuple, orig_binding, callback){

  if(!_.isArray(tuple)){
    return callback(new Error("tuple must be an array"));
  }
  if(!_.isPlainObject(orig_binding)){
    return callback(new Error("binding must be a plain object"));
  }

  parseTuple(fb.hindex, bindToTuple(tuple, orig_binding), function(err, q_fact){
    if(err){
      if(err.type === 'NotFoundError'){
        //one of the tuple values were not found in the hash, so there must be no results
        return callback(null, []);
      }
      return callback(err);
    }
    var index_to_use = selectIndex(q_fact);

    var hash_facts = [];
    forEachMatchingHashFact(fb, toMatcher(index_to_use, q_fact), function(hash_fact){
      hash_facts.push(hash_fact);
    }, function(err){
      if(err) return callback(err);

      var bindings = bindHashFacts(fb, hash_facts, q_fact);

      //de-hash the bindings
      λ.map(bindings, function(binding, callback){
        λ.map(_.pairs(binding), function(p, callback){
          if(_.isString(p[1])){
            fb.hindex.get(p[1], function(err, val){
              callback(err, [p[0], val]);
            });
          }else{
            callback(null, [p[0], p[1]]);
          }
        }, function(err, pairs){
          callback(err, _.assign({}, orig_binding, _.object(pairs)));
        });
      }, callback);
    });
  });
};

var q = function(fb, tuples, bindings, callback){
  if(!_.isArray(tuples)){
    return callback(new Error("q expects an array of tuples"));
  }
  if(!_.isArray(bindings)){
    return callback(new Error("q expects an array bindings"));
  }

  var memo = bindings;
  λ.each.series(tuples, function(tuple, callback){
    λ.map(memo, function(binding, callback){
      qTuple(fb, tuple, binding, callback);
    }, function(err, next_bindings){
      if(err) return callback(err);
      memo = _.flatten(next_bindings);
      callback();
    });
  }, function(err){
    if(err) callback(err);
    else callback(null, memo);
  });
};

module.exports = {
  q: q,
  qTuple: qTuple,
  getEntity: function(fb, e, callback){
    q(fb, [["?e", "?a", "?v"]], [{"?e": e}], function(err, results){
      if(err) return callback(err);
      var o = {};
      results.forEach(function(result){
        var a = result["?a"];
        if(isMultiValued(fb, a)){
          if(!_.isArray(o[a])){
            o[a] = [];
          }
          o[a].push(result["?v"]);
        }else{
          o[a] = result["?v"];
        }
      });
      callback(null, o);
    });
  }
};
