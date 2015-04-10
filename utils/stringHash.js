var toPaddedBase36 = require("./toPaddedBase36");

var hashCode = function(str){
  var hash = 0;
  var i;
  for(i = 0; i < str.length; i++){
    hash = 31 * hash + str.charCodeAt(i) | 0;
  }
  return ((hash >>> 1) & 0x40000000) | (hash & 0xBFFFFFFF);
};

module.exports = function(str){
  var h = hashCode(str);
  return toPaddedBase36(h, 6, true);
};
