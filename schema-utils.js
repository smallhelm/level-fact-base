var isAttributeMultiValued_THIS_MAY_THROWUP = function(fb, a){
  if(!fb.schema.hasOwnProperty(a) || !fb.schema[a]){
    throw new Error("Attribute not found: " + a);
  }
  return !!fb.schema[a]["_db/is-multi-valued"];
};

var getAttributeFromHash_THIS_MAY_THROWUP = function(fb, h){
  var hashes = fb.schema["_db/attribute-hashes"] || {};
  if(!hashes.hasOwnProperty(h) || !hashes[h]){
    throw new Error("Attribute not found for hash: " + h);
  }
  return hashes[h];
};

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

  isAttributeMultiValued_THIS_MAY_THROWUP: isAttributeMultiValued_THIS_MAY_THROWUP,

  isAttributeHashMultiValued_THIS_MAY_THROWUP: function(fb, h){
    var a = getAttributeFromHash_THIS_MAY_THROWUP(fb, h);
    return isAttributeMultiValued_THIS_MAY_THROWUP(fb, a);
  },
  getAttributeFromHash_THIS_MAY_THROWUP: getAttributeFromHash_THIS_MAY_THROWUP,
  getIndexesForAttribute: function(fb, a){
    //TODO implement the right way
    return [
      'eavto',
      'aevto',
      'teavo'//log
    ];
  }
};
