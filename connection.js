var HashIndex = require('level-hash-index');

module.exports = function(db, options){
  options = options || {};

  var hindex = options.hindex || HashIndex(db);

  return {
    snap: function(){
      return {
        db: db,
        hindex: hindex
      };
    }
  };
};
