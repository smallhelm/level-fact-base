module.exports = function(n, len, ensure_sign){
	var s = n.toString(36);
	s = n < 0 ? s.substring(1) : s;
	while(s.length < len){
		s = '0' + s;
	}
	return (n < 0 ? '-' : (ensure_sign ? '+' : '')) + s;
};
