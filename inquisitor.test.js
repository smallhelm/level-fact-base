var _ = require('lodash');
var λ = require('contra');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var genRandomString = require('./utils/genRandomString');

var Inquisitor = require('./inquisitor');
var Transactor = require('./transactor');

var setupMiddleDataset = function(callback){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return callback(err);
    transactor.transact([
      [    "axl", "father",     "mike"],
      [    "axl", "mother",  "frankie"],
      [    "sue", "father",     "mike"],
      [    "sue", "mother",  "frankie"],
      [  "brick", "father",     "mike"],
      [  "brick", "mother",  "frankie"],
      [   "mike", "father", "big mike"],
      [  "rusty", "father", "big mike"],
      ["frankie", "mother",      "pat"],
      ["frankie", "father",      "tag"],
      [  "janet", "mother",      "pat"],
      [  "janet", "father",      "tag"]
    ], {}, function(err){
      if(err) callback(err);
      else callback(null, Inquisitor(db));
    });
  });
};

test("basic qTuple stuff", function(t){
  setupMiddleDataset(function(err, inq){
    if(err) return t.end(err);
    λ.concurrent({
      axl_mother:           λ.curry(inq.qTuple, ["axl",       "mother", "?mother"], {}),
      axl_relation_to_mike: λ.curry(inq.qTuple, ["axl",    "?relation", "mike"], {}),
      mikes_children:       λ.curry(inq.qTuple, ["?children", "father", "?father"], {"?father": "mike"}),
      axl_has_no_children:  λ.curry(inq.qTuple, ["?children", "father", "axl"], {})
    }, function(err, r){
      t.deepEqual(_.pluck(r.axl_mother, "?mother"), ["frankie"]);
      t.deepEqual(_.pluck(r.axl_relation_to_mike, "?relation"), ["father"]);
      t.deepEqual(_.pluck(r.mikes_children, "?children").sort(), ["axl", "brick", "sue"]);
      t.equal(r.axl_has_no_children.length, 0);
      t.end(err);
    });
  });
});

test("do some family tree questions", function(t){
  setupMiddleDataset(function(err, inq){
    if(err) return t.end(err);
    λ.concurrent({
      husbands_and_wifes:   λ.curry(inq.q, [["?child", "mother", "?wife"],
                                                ["?child", "father", "?husband"]], [{}]),

      sue_grandfathers:     λ.curry(inq.q, [[    "sue", "father", "?father"],
                                                [    "sue", "mother", "?mother"],
                                                ["?mother", "father", "?grandpa1"],
                                                ["?father", "father", "?grandpa2"]], [{}]),

      sue_siblings:         λ.curry(inq.q, [[    "?sue", "mother", "?mother"],
                                                ["?sibling", "mother", "?mother"]], [{"?sue": "sue"}]),
    }, function(err, r){
      t.deepEqual(_.unique(_.map(r.husbands_and_wifes, function(result){
        return result["?husband"] + " & " + result["?wife"]
      }).sort()), ["mike & frankie", "tag & pat"]);
      t.deepEqual(_.unique(_.pluck(r.sue_grandfathers, "?grandpa1").concat(_.pluck(r.sue_grandfathers, "?grandpa2"))).sort(), ["big mike", "tag"]);
      t.deepEqual(_.unique(_.pluck(r.sue_siblings, "?sibling")).sort(), ["axl", "brick", "sue"]);
      t.end(err);
    });
  });
});

test("queries using txn", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);
    λ.series([
      λ.curry(transactor.transact, [["prophet", "is",    "smith"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",    "young"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",   "taylor"]], {}),
      λ.curry(transactor.transact, [["prophet", "is", "woodruff"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",     "snow"]], {})
    ], function(err){
      if(err) return t.end(err);
      var inq = Inquisitor(db);
      λ.concurrent({
        first:          λ.curry(inq.q, [["prophet", "is", "?name",      1]], [{}]),
        third:          λ.curry(inq.q, [["prophet", "is", "?name",      3]], [{}]),
        when_was_young: λ.curry(inq.q, [["prophet", "is", "young", "?txn"]], [{}]),
        who_is_latest:  λ.curry(inq.q, [["prophet", "is", "?name"        ]], [{}]),
        names_in_order: λ.curry(inq.q, [["prophet", "is", "?name", "?txn"]], [{}])
      }, function(err, r){
        t.deepEqual(_.pluck(r.first, "?name"), ["smith"]);
        t.deepEqual(_.pluck(r.third, "?name"), ["taylor"]);
        t.deepEqual(_.pluck(r.when_was_young, "?txn"), [2]);
        t.deepEqual(_.pluck(r.who_is_latest, "?name"), ["snow"]);
        t.deepEqual(_.pluck(_.sortBy(r.names_in_order, "?txn"), "?name"), ["smith", "young", "taylor", "woodruff", "snow"]);
        t.end(err);
      });
    });
  });
});

test("getEntity", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);
    λ.series([
      λ.curry(transactor.transact, [["u0", "email", "andy@email.com"],
                                        ["u0", "name",  "andy"]], {}),

      λ.curry(transactor.transact, [["u1", "email", "opie@email.com"],
                                        ["u1", "name",  "opie"]], {}),

      λ.curry(transactor.transact, [["u0", "email", "new@email.com"]], {})
    ], function(err){
      if(err) return t.end(err);
      var inq = Inquisitor(db);
      λ.concurrent({
        u0: λ.curry(inq.getEntity, "u0"),
        u1: λ.curry(inq.getEntity, "u1")
      }, function(err, r){
        t.deepEqual(r.u0, {name: "andy", email: "new@email.com"});
        t.deepEqual(r.u1, {name: "opie", email: "opie@email.com"});
        t.end(err);
      });
    });
  });
});
