module.exports = {
  getTypeForAttribute_THIS_MAY_THROWUP: function(fb, a){
    if(!fb.schema.hasOwnProperty(a) || !fb.schema[a]){
      throw new Error("Attribute not found: " + a);
    }
    var type = fb.schema[a]["_db/type"] || 'String';
    if(!fb.types.hasOwnProperty(type)){
      throw new Error("Attribute " + a + " has an unknown type: " + type);
    }
    return fb.types[type];
  },
  getIndexesForAttribute: function(fb, a){
    //TODO implement the right way
    return [
      'eavto',
      'aevto',
      'teavo'//log
    ];
  }
};
