var genInt = require('gent/generator/integer');
var genString = require('gent/generator/string');

var nextStr = genString(genInt(0, 1000));

module.exports = function(){
	return nextStr.next().value;
};
