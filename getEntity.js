var _ = require('lodash');
var q = require('./inquisitor').q;
var assertFB = require('./utils/assertFB');
var SchemaUtils = require('./schema-utils');

var isMultiValued = function(fb, a){
  try{
    return SchemaUtils.isAttributeMultiValued_THIS_MAY_THROWUP(fb, a);
  }catch(e){
    return false;
  }
};

module.exports = function(fb, e, callback){
  try{assertFB(fb);}catch(e){return callback(e);}

  q(fb, [["?e", "?a", "?v"]], [{"?e": e}], function(err, results){
    if(err) return callback(err);
    if(results.length === 0){
      return callback(null, null);
    }
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
};
