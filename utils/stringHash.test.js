var _ = require('lodash');
var test = require('tape');
var strHash = require('./stringHash');
var genRandomString = require('./genRandomString');
 
test("assert all string-hashes are the same length and alpha-numeric", function(t){
	var hashes = _.map(_.range(0, 10000), function(){
		return strHash(genRandomString());
	});

	t.ok(_.every(hashes, RegExp.prototype.test, /^[-+][0-9a-z]+$/), "all hashes should be signed base36 strings");
	t.equal(1, _.unique(hashes.map(_.size)).length, "all hashes should be the same length");
	t.end();
});
