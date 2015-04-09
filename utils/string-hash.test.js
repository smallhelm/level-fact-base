var _ = require('lodash');
var test = require('tape');
var hash = require('./string-hash');
var genInt = require('gent/generator/integer');
var genString = require('gent/generator/string');
 
var nextStr = genString(genInt(0, 100));

var randomString = function(){
	return nextStr.next().value;
};

var n_tests = 10000;

test("assert all string-hashes are the same length and alpha-numeric", function(t){
	t.plan(n_tests + 1);

	t.equal(1, _.unique(_.map(_.range(0, n_tests), function(){
		var str = hash(randomString());
		t.ok(/^[-+][0-9a-z]+/.test(str));
		return str.length;
	})).length);
});

