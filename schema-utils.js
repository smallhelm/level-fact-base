module.exports = {
  getTypeForAttribute: function(fb, a){
    if(!fb.schema.hasOwnProperty(a) || !fb.schema[a]){
      return null;
    }
    return fb.types[fb.schema[a]["_db/type"] || 'String'] || null;
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
