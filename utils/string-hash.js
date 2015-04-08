var hashCode = function(str){
	var hash = 0;
	var i;
	for(i = 0; i < str.length; i++){
		hash = 31 * hash + str.charCodeAt(i) | 0;
	}
	return ((hash >>> 1) & 0x40000000) | (hash & 0xBFFFFFFF);
};

var hashNumToString = function(hash){
	var s = hash.toString(36);
	s = hash < 0 ? s.substring(1) : s;
	while(s.length < 6){
		s = '0' + s;
	}
	return (hash < 0 ? '-' : '+') + s;
};

module.exports = function(str){
	var h = hashCode(str);
	return hashNumToString(h);
};
