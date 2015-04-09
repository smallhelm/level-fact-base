var _ = require('lodash');
var test = require('tape');
var async = require('async');
var level = require('levelup');
var memdown = require('memdown');
var strHash = require('./utils/stringHash');
var HashTable = require('./level-hash-table');
var genRandomString = require('./utils/genRandomString');

test("ensure the basics work", function(t){
	var db = level(memdown);
	var htable = HashTable(db);
	var expected_hash_key = strHash("hello") + "00";

	htable.get(expected_hash_key, function(err, val){
		t.equal(err && err.type, 'NotFoundError');
		t.notOk(val);

		async.series([
			async.apply(htable.putAndWrite, "hello"),
			async.apply(htable.get, expected_hash_key),
			async.apply(htable.getHash, "hello")
		], function(err, results){
			t.equal(results[0], expected_hash_key);
			t.equal(results[1], "hello");
			t.equal(results[2], expected_hash_key);
			t.end(err);
		});
	});
});

test("ensure re-putting the same value before write yields the same hash", function(t){
	var htable = HashTable(level(memdown));

	async.series([
		async.apply(htable.put, "hello"),
		async.apply(htable.put, "hello")
	], function(err, results){
		t.equal(results[0].is_new, true);
		t.notOk(results[1].is_new, "since the first one added it to the cache, we already have it");
		t.ok(_.isString(results[0].hash));
		t.ok(_.isString(results[1].hash), JSON.stringify(results[1]));
		t.equal(results[0].hash, results[1].hash);
		t.end(err);
	});
});

var hashingThatAlwaysCollides = function(){
	return "notahash";
};

test("handle hash collisions that are persisted", function(t){
	var htable = HashTable(level(memdown), {
		hashFn: hashingThatAlwaysCollides,
		hash_seq_length: 2
	});

	var vals = _.unique(_.map(_.range(0, 100), genRandomString));

	async.series(vals.map(function(val){
		return async.apply(htable.putAndWrite, val);
	}), function(err, hashes){
		t.ok(_.every(hashes, _.isString), "assert all hashes are string");
		t.equal(hashes.length, _.unique(hashes).length, "assert no collisions");
		t.equal(1, _.unique(hashes.map(function(hash){
			return hash.substring(0, hash.length - 2);
		})).length, "assert they actually did collide");
		t.end(err);
	});
});

test("handle hash collisions that are not yet persisted", function(t){
	var htable = HashTable(level(memdown), {
		hashFn: hashingThatAlwaysCollides,
		hash_seq_length: 2
	});

	var vals = _.unique(_.map(_.range(0, 100), genRandomString));

	async.series(vals.map(function(val){
		return async.apply(htable.put, val);
	}), function(err, results){
		var hashes = _.pluck(results, "hash");
		t.ok(_.every(_.pluck(results, "is_new")), "assert all are new hashes");
		t.ok(_.every(hashes, _.isString), "assert all hashes are string");
		t.equal(hashes.length, _.unique(hashes).length, "assert no collisions");
		t.equal(1, _.unique(hashes.map(function(hash){
			return hash.substring(0, hash.length - 2);
		})).length, "assert they actually did collide");
		t.end(err);
	});
});
