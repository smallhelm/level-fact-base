var _ = require('lodash');
var λ = require('contra');
var qTuple = require('./inquisitor').qTuple;
var assertFB = require('./utils/assertFB');

module.exports = function(fb, tuples, bindings, callback){
  if(arguments.length === 3){
    callback = bindings;
    bindings = [{}];
  }
  try{assertFB(fb);}catch(e){return callback(e);}
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
