var _ = require('lodash');
var λ = require('contra');
var HashIndex = require('level-hash-index');
var SchemaUtils = require('./schema-utils');
var escapeRegExp = require('escape-regexp');
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

var parseElementThroughHIndex = function(fb, elm, callback){
  elm = unEscapeVar(elm);
  fb.hindex.getHash(elm, function(err, hash){
    if(err) callback(err);
    else callback(null, {hash: hash});
  });
}

var parseElement = function(fb, tuple, i, callback){
  var elm = tuple.length < i + 1 ? '?_' : tuple[i];
  if(isTheThrowAwayVar(elm)){
    callback(null, {is_blank: true});
  }else if(isVar(elm)){
    callback(null, {var_name: elm});
  }else if(i < 2 && _.isString(elm)){
    parseElementThroughHIndex(fb, elm, callback);
  }else if(i === 2){
    var type = getTypeForAttribute(fb, tuple[1]);
    if(!type){
      var type_not_yet_known = {};
      λ.each(Object.keys(fb.types), function(type_name, next){
        var type = fb.types[type_name];
        if(type.validate(elm)){
          parseElementThroughHIndex(fb, type.encode(elm), function(err, o){
            if(err) return next(err);
            type_not_yet_known[type_name] = o.hash;
            next(null);
          });
        }else{
          next(null);//just ignore it
        }
      }, function(err){
        if(err) return callback(err);

        if(_.size(type_not_yet_known) === 0){
          callback(new Error('value in tuple has invalid type'));
        }else if(_.size(type_not_yet_known) === 1){
          callback(null, {hash: _.first(_.values(type_not_yet_known))});
        }else{
          callback(null, {type_not_yet_known: type_not_yet_known});
        }
      });
    }else{
      if(type.validate(elm)){
        parseElementThroughHIndex(fb, type.encode(elm), callback);
      }else{
        callback(new Error('value in tuple has invalid type'));
      }
    }
  }else if(i === 3 && _.isNumber(elm)){
    var txn = toPaddedBase36(elm, 6);
    callback(null, {hash: txn});
  }else if(i === 4 && (elm === true || elm === false)){
    callback(null, {hash: elm});
  }else{
    callback(new Error('element ' + i + ' in tuple has invalid type'));
  }
};

var parseTuple = function(fb, tuple, callback){
  λ.concurrent({
    e: λ.curry(parseElement, fb, tuple, 0),
    a: λ.curry(parseElement, fb, tuple, 1),
    v: λ.curry(parseElement, fb, tuple, 2),
    t: λ.curry(parseElement, fb, tuple, 3),
    o: λ.curry(parseElement, fb, tuple, 4)
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

  var key_regex = new RegExp(escapeRegExp(prefix) + index_to_use.split("").map(function(k){
    if(q_fact[k].hasOwnProperty('hash')){
      if(!found_a_gap){
        prefix_parts.push(q_fact[k].hash);
      }
      return escapeRegExp(q_fact[k].hash);
    }else{
      found_a_gap = true;
      return '.*';
    }
  }).join(escapeRegExp('!')));

  return {
    prefix: prefix + prefix_parts.join('!'),
    getHashFactIfKeyMatches: function(fb, key){
      if(!key_regex.test(key)){
        return false;
      }
      var hash_fact = parseKey(key);
      if(hash_fact.t > fb.txn){
        return false;//this fact is too new, so ignore it
      }
      if(q_fact.v.hasOwnProperty("type_not_yet_known")){
        var type_name = getTypeNameForHash(fb, hash_fact.a);
        if(!q_fact.v.type_not_yet_known.hasOwnProperty(type_name)){
          return false;//just ignore this fact b/c types don't line up
        }
        if(q_fact.v.type_not_yet_known[type_name] !== hash_fact.v){
          return false;//just ignore this fact b/c it's not the value the user specified
        }
      }
      return hash_fact;
    }
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
    var hash_fact = matcher.getHashFactIfKeyMatches(fb, key);
    if(!hash_fact){
      return;//just ignore and keep going
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

var isHashMultiValued = function(fb, h){
  try{
    return SchemaUtils.isAttributeHashMultiValued_THIS_MAY_THROWUP(fb, h);
  }catch(e){
    return false;
  }
};

var getTypeForAttribute = function(fb, a){
  try{
    return SchemaUtils.getTypeForAttribute_THIS_MAY_THROWUP(fb, a);
  }catch(e){
    return null;
  }
};

var getTypeForHash = function(fb, h){
  try{
    var a = SchemaUtils.getAttributeFromHash_THIS_MAY_THROWUP(fb, h);
    return getTypeForAttribute(fb, a);
  }catch(e){
    return null;
  }
};

var getTypeNameForHash = function(fb, h){
  try{
    var a = SchemaUtils.getAttributeFromHash_THIS_MAY_THROWUP(fb, h);
    return SchemaUtils.getTypeNameForAttribute_THIS_MAY_THROWUP(fb, a);
  }catch(e){
    return null;
  }
};

var SetOfBindings = function(fb, q_fact){

  var only_the_latest = q_fact.t.is_blank;
  if(isHashMultiValued(fb, q_fact.a.hash)){
    only_the_latest = false;
  }
  var is_attribute_unknown = q_fact.a.hasOwnProperty('var_name') || q_fact.a.hasOwnProperty('is_blank');

  var var_names = "eavto".split('').filter(function(k){
    return q_fact[k].hasOwnProperty('var_name');
  }).map(function(k){
    return [q_fact[k].var_name, k];
  });

  var set = {};
  var latest_for = {};//latest for the same e+a

  return {
    add: function(hash_fact){
      if(only_the_latest && is_attribute_unknown){
        only_the_latest = !isHashMultiValued(fb, hash_fact.a);
      }
      var type = getTypeForHash(fb, is_attribute_unknown ? hash_fact.a : q_fact.a.hash);

      var key_for_latest_for = only_the_latest ? hash_fact.e + hash_fact.a : _.uniqueId();

      if(latest_for.hasOwnProperty(key_for_latest_for)){
        if(latest_for[key_for_latest_for].txn > hash_fact.t){
          return;//not the latest, so skip the rest
        }
      }
      var binding = {};
      var hash_key = '';//to ensure uniqueness
      var_names.forEach(function(p){
        var k = p[1];
        if(k === 'v'){
          binding[p[0]] = {
            hash: hash_fact[k],
            decode: type.decode
          };
        }else{
          binding[p[0]] = hash_fact[k];
        }
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

var qTuple = function(fb, tuple, orig_binding, callback){

  if(!_.isArray(tuple)){
    return callback(new Error("tuple must be an array"));
  }
  if(!_.isPlainObject(orig_binding)){
    return callback(new Error("binding must be a plain object"));
  }

  parseTuple(fb, bindToTuple(tuple, orig_binding), function(err, q_fact){
    if(err){
      if(err.type === 'NotFoundError'){
        //one of the tuple values were not found in the hash, so there must be no results
        return callback(null, []);
      }
      return callback(err);
    }
    var index_to_use = selectIndex(q_fact);

    var is_attribute_unknown = q_fact.a.hasOwnProperty('var_name');

    var s = SetOfBindings(fb, q_fact);
    forEachMatchingHashFact(fb, toMatcher(index_to_use, q_fact), function(hash_fact){
      s.add(hash_fact);
    }, function(err){
      if(err) return callback(err);

      var hash_bindings = s.toArray();

      //de-hash the bindings
      λ.map(hash_bindings, function(binding, callback){
        λ.map(_.pairs(binding), function(p, callback){
          var var_name = p[0];
          var var_value = p[1];
          var decode = _.identity;
          if(var_value && var_value.decode){
            decode = var_value.decode;
            var_value = var_value.hash;
          }
          if(_.isString(var_value)){
            fb.hindex.get(var_value, function(err, val){
              callback(err, [var_name, decode(val)]);
            });
          }else{
            callback(null, [var_name, var_value]);
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
