var _ = require('lodash');
var test = require('tape');
var async = require('async');
var level = require('levelup');
var memdown = require('memdown');
var strHash = require('./utils/stringHash');
var HashIndex = require('./level-hash-index');
var genRandomString = require('./utils/genRandomString');

test("ensure the basics work", function(t){
  var db = level(memdown);
  var hindex = HashIndex(db);
  var expected_hash_key = strHash("hello") + "00";

  hindex.get(expected_hash_key, function(err, val){
    t.equal(err && err.type, 'NotFoundError');
    t.notOk(val);

    async.series([
      async.apply(hindex.putAndWrite, "hello"),
      async.apply(hindex.get, expected_hash_key),
      async.apply(hindex.getHash, "hello")
    ], function(err, results){
      t.equal(results[0], expected_hash_key);
      t.equal(results[1], "hello");
      t.equal(results[2], expected_hash_key);
      t.end(err);
    });
  });
});

test("ensure re-putting the same value before write yields the same hash", function(t){
  var hindex = HashIndex(level(memdown));

  var n_puts = 100;
  async.parallel(_.range(0, n_puts).map(function(){
    return async.apply(hindex.put, "hello");
  }), function(err, results){
    t.deepEqual(_.mapValues(_.groupBy(results, function(result){
      return result.is_new === true;
    }), _.size), {'true': 1, 'false': n_puts - 1}, "only the first put should be new");

    var hashes = _.pluck(results, "hash");
    t.ok(_.every(hashes, _.isString), "assert all hashes are string");
    t.equal(1, _.unique(hashes).length, "assert only one hash");
    t.end(err);
  });
});

var hashingThatAlwaysCollides = function(){
  return "notahash";
};

test("handle hash collisions that are persisted", function(t){
  var hindex = HashIndex(level(memdown), {
    hashFn: hashingThatAlwaysCollides,
    hash_seq_length: 2
  });

  var vals = _.unique(_.map(_.range(0, 100), genRandomString));

  async.series(vals.map(function(val){
    return async.apply(hindex.putAndWrite, val);
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
  var hindex = HashIndex(level(memdown), {
    hashFn: hashingThatAlwaysCollides,
    hash_seq_length: 2
  });

  var vals = _.unique(_.map(_.range(0, 100), genRandomString));

  async.series(vals.map(function(val){
    return async.apply(hindex.put, val);
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
