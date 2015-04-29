module.exports = {
  getTypeForAttribute: function(fb, a, callback){
    //TODO implement the right way

    if(!fb.types["String"].validate(a) || !fb.schema.hasOwnProperty(a)){
      return callback(new Error("Attribute not found in schema: " + a));
    }
    callback(null, fb.types[fb.schema[a]["_db/type"]]);
  },
  getIndexesForAttribute: function(fb, a, callback){
    //TODO implement the right way
    callback(null, [
      'eavto',
      'aevto',
      'teavo'//log
    ]);
  }
};
