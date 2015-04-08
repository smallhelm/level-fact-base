var _ = require('lodash');
var test = require('tape');
var hash = require('./string-hash');
 
var randomChar = (function(){
	var chars = "\"'~!@#$%^&*()<:>[]{}.,+=-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".split("");
	return function(){
		return _.sample(chars);
	};
}());

var randomString = function(){
	return _.map(_.range(1, _.random(0, 10000)), randomChar).join("");
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

