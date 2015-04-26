var _ = require('lodash');
var λ = require('contra');
var inq = require('./inquisitor');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var Transactor = require('./transactor');
var Connection = require('./connection');
var genRandomString = require('./utils/genRandomString');

var setupMiddleDataset = function(callback){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return callback(err);
    var conn = Connection(db);

    transactor.transact([
      ["01", "_db/attribute", "father"],
      ["01", "_db/type"     , "String"],

      ["02", "_db/attribute", "mother"],
      ["02", "_db/type"     , "String"]
    ], {}, function(err){
      if(err) callback(err);

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
        else callback(null, conn.snap());

      });
    });
  });
};

test("basic qTuple stuff", function(t){
  setupMiddleDataset(function(err, fb){
    if(err) return t.end(err);
    λ.concurrent({
      axl_mother:           λ.curry(inq.qTuple, fb, ["axl",       "mother", "?mother"], {}),
      axl_relation_to_mike: λ.curry(inq.qTuple, fb, ["axl",    "?relation", "mike"], {}),
      mikes_children:       λ.curry(inq.qTuple, fb, ["?children", "father", "?father"], {"?father": "mike"}),
      axl_has_no_children:  λ.curry(inq.qTuple, fb, ["?children", "father", "axl"], {})
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
  setupMiddleDataset(function(err, fb){
    if(err) return t.end(err);
    λ.concurrent({
      husbands_and_wifes:   λ.curry(inq.q, fb, [["?child", "mother", "?wife"],
                                                ["?child", "father", "?husband"]], [{}]),

      sue_grandfathers:     λ.curry(inq.q, fb, [[    "sue", "father", "?father"],
                                                [    "sue", "mother", "?mother"],
                                                ["?mother", "father", "?grandpa1"],
                                                ["?father", "father", "?grandpa2"]], [{}]),

      sue_siblings:         λ.curry(inq.q, fb, [[    "?sue", "mother", "?mother"],
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
    var conn = Connection(db);
    λ.series([
      λ.curry(transactor.transact, [["01", "_db/attribute", "is"],
                                    ["01", "_db/type"     , "String"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",    "smith"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",    "young"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",   "taylor"]], {}),
      λ.curry(transactor.transact, [["prophet", "is", "woodruff"]], {}),
      λ.curry(transactor.transact, [["prophet", "is",     "snow"]], {})
    ], function(err){
      if(err) return t.end(err);
      var fb = conn.snap();
      λ.concurrent({
        first:          λ.curry(inq.q, fb, [["prophet", "is", "?name",      2]], [{}]),
        third:          λ.curry(inq.q, fb, [["prophet", "is", "?name",      4]], [{}]),
        when_was_young: λ.curry(inq.q, fb, [["prophet", "is", "young", "?txn"]], [{}]),
        who_is_latest:  λ.curry(inq.q, fb, [["prophet", "is", "?name"        ]], [{}]),
        names_in_order: λ.curry(inq.q, fb, [["prophet", "is", "?name", "?txn"]], [{}])
      }, function(err, r){
        t.deepEqual(_.pluck(r.first, "?name"), ["smith"]);
        t.deepEqual(_.pluck(r.third, "?name"), ["taylor"]);
        t.deepEqual(_.pluck(r.when_was_young, "?txn"), [3]);
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
    var conn = Connection(db);
    λ.series([
      λ.curry(transactor.transact, [["01", "_db/attribute", "email"],
                                    ["01", "_db/type"     , "String"],
                                    ["02", "_db/attribute", "name"],
                                    ["02", "_db/type"     , "String"]], {}),

      λ.curry(transactor.transact, [["u0", "email", "andy@email.com"],
                                    ["u0", "name",  "andy"]], {}),

      λ.curry(transactor.transact, [["u1", "email", "opie@email.com"],
                                    ["u1", "name",  "opie"]], {}),

      λ.curry(transactor.transact, [["u0", "email", "new@email.com"]], {})
    ], function(err){
      if(err) return t.end(err);
      var fb = conn.snap();
      λ.concurrent({
        u0: λ.curry(inq.getEntity, fb, "u0"),
        u1: λ.curry(inq.getEntity, fb, "u1")
      }, function(err, r){
        t.deepEqual(r.u0, {name: "andy", email: "new@email.com"});
        t.deepEqual(r.u1, {name: "opie", email: "opie@email.com"});
        t.end(err);
      });
    });
  });
});

test("the throw-away binding", function(t){
  setupMiddleDataset(function(err, fb){
    if(err) return t.end(err);
    λ.concurrent({
      all_entities: λ.curry(inq.q, fb, [["?entity"]], [{}]),
      all_fathers:  λ.curry(inq.q, fb, [["?_", "father", "?father"]], [{}]),
      sue_siblings: λ.curry(inq.q, fb, [[    "?sue", "mother", "?_"],
                                        ["?sibling", "mother", "?_"]], [{"?sue": "sue"}])
    }, function(err, r){
      t.deepEqual(_.pluck(r.all_entities, "?entity").sort(), ['01', '02', '_txid000001', '_txid000002', 'axl', 'brick', 'frankie', 'janet', 'mike', 'rusty', 'sue']);
      t.deepEqual(_.sortBy(r.all_fathers, "?father"), [{"?father": 'big mike'}, {"?father": 'mike'}, {"?father": 'tag'}], "should not have ?_ bound to anything");
      t.deepEqual(_.sortBy(r.sue_siblings, "?sibling"), [{'?sibling': 'axl', '?sue': 'sue'},
                                                         {'?sibling': 'brick', '?sue': 'sue'},
                                                         {'?sibling': 'frankie', '?sue': 'sue'},
                                                         {'?sibling': 'janet', '?sue': 'sue'},
                                                         {'?sibling': 'sue', '?sue': 'sue'}], "should be everyone with a mother b/c ?_ shouldn't join");
      t.end(err);
    });
  });
});

test("escaping '?...' values", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);
    var conn = Connection(db);
    λ.series([
      λ.curry(transactor.transact, [["0", "_db/attribute", "name"],
                                    ["0", "_db/type"     , "String"]], {}),

      λ.curry(transactor.transact, [["1", "name", "?notavar"],
                                    ["2", "name", "notavar"],
                                    ["3", "name", "\\?notavar"],
                                    ["4", "name", "\\\\"],
                                    ["5", "name", "?_"]], {})
    ], function(err){
      if(err) return t.end(err);
      var fb = conn.snap();
      λ.concurrent({
        should_be_a_var:      λ.curry(inq.q, fb, [["?id", "name", "?notavar"]], [{}]),
        bind_it:              λ.curry(inq.q, fb, [["?id", "name", "?name"]], [{"?name": "?notavar"}]),
        escape_it:            λ.curry(inq.q, fb, [["?id", "name", "\\?notavar"]], [{}]),
        bind_it2:             λ.curry(inq.q, fb, [["?id", "name", "?name"]], [{"?name": "\\?notavar"}]),
        not_actually_escaped: λ.curry(inq.q, fb, [["?id", "name", "\\\\?notavar"]], [{}]),
        double_slash:         λ.curry(inq.q, fb, [["?id", "name", "\\\\\\"]], [{}]),
        double_slash_bind:    λ.curry(inq.q, fb, [["?id", "name", "?name"]], [{"?name": "\\\\"}]),
        not_a_throw_away:     λ.curry(inq.q, fb, [["?id", "name", "\\?_"]], [{}]),
        not_a_throw_away2:    λ.curry(inq.q, fb, [["?id", "name", "?name"]], [{"?name": "?_"}]),
      }, function(err, r){
        t.deepEqual(r.should_be_a_var, [{"?id": "1", "?notavar": "?notavar"}, {"?id": "2", "?notavar": "notavar"}, {"?id": "3", "?notavar": "\\?notavar"}, {"?id": "4", "?notavar": "\\\\"}, {"?id": "5", "?notavar": "?_"}]);
        t.deepEqual(r.bind_it, [{"?id": "1", "?name": "?notavar"}]);
        t.deepEqual(r.escape_it, [{"?id": "1"}]);
        t.deepEqual(r.bind_it2, [{"?id": "3", "?name": "\\?notavar"}]);
        t.deepEqual(r.not_actually_escaped, [{"?id": "3"}]);
        t.deepEqual(r.double_slash, [{"?id": "4"}]);
        t.deepEqual(r.double_slash_bind, [{"?id": "4", "?name": "\\\\"}]);
        t.deepEqual(r.not_a_throw_away, [{"?id": "5"}]);
        t.deepEqual(r.not_a_throw_away2, [{"?id": "5", "?name": "?_"}]);
        t.end(err);
      });
    });
  });
});
