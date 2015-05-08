var getAttributeDefinition_THIS_MAY_THROWUP = function(fb, a){
  if(!fb.schema.hasOwnProperty(a) || !fb.schema[a]){
    throw new Error("Attribute not found: " + a);
  }
  return fb.schema[a];
};

var isAttributeMultiValued_THIS_MAY_THROWUP = function(fb, a){
  return !!getAttributeDefinition_THIS_MAY_THROWUP(fb, a)["_db/is-multi-valued"];
};

var getAttributeFromHash_THIS_MAY_THROWUP = function(fb, h){
  var hashes = fb.schema["_db/attribute-hashes"] || {};
  if(!hashes.hasOwnProperty(h) || !hashes[h]){
    throw new Error("Attribute not found for hash: " + h);
  }
  return hashes[h];
};

var getTypeNameForAttribute_THIS_MAY_THROWUP = function(fb, a){
  var type_name = getAttributeDefinition_THIS_MAY_THROWUP(fb, a)["_db/type"] || 'String';
  if(!fb.types.hasOwnProperty(type_name)){
    throw new Error("Attribute " + a + " has an unknown type: " + type_name);
  }
  return type_name;
};

module.exports = {
  getTypeNameForAttribute_THIS_MAY_THROWUP: getTypeNameForAttribute_THIS_MAY_THROWUP,
  getTypeForAttribute_THIS_MAY_THROWUP: function(fb, a){
    var type_name = getTypeNameForAttribute_THIS_MAY_THROWUP(fb, a);
    return fb.types[type_name];
  },

  isAttributeMultiValued_THIS_MAY_THROWUP: isAttributeMultiValued_THIS_MAY_THROWUP,

  isAttributeHashMultiValued_THIS_MAY_THROWUP: function(fb, h){
    var a = getAttributeFromHash_THIS_MAY_THROWUP(fb, h);
    return isAttributeMultiValued_THIS_MAY_THROWUP(fb, a);
  },
  getAttributeFromHash_THIS_MAY_THROWUP: getAttributeFromHash_THIS_MAY_THROWUP
};
