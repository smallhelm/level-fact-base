var _ = require('lodash');
var λ = require('contra');
var inq = require('./inquisitor');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var Transactor = require('./transactor');
var genRandomString = require('./utils/genRandomString');

var setupMiddleDataset = function(callback){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return callback(err);

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
      ], {}, callback);
    });
  });
};

var prophets = ["smith", "young", "taylor", "woodruff", "snow", "f. smith", "grant", "a. smith", "mckay", "fielding smith", "lee", "kimball", "benson", "hunter", "hinckley", "monson"];
var setupProphetDataset = function(callback){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return callback(err);
    λ.series([
      λ.curry(transactor.transact, [["01", "_db/attribute", "is"],
                                    ["01", "_db/type"     , "String"]], {})
    ].concat(prophets.map(function(name){
      return λ.curry(transactor.transact, [["prophet", "is", name]], {});
    })), callback);
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
  setupProphetDataset(function(err, fb_versions){
    if(err) return t.end(err);
    var fb = _.last(fb_versions);
    λ.concurrent({
      first:          λ.curry(inq.q, fb, [["prophet", "is", "?name",      2]], [{}]),
      third:          λ.curry(inq.q, fb, [["prophet", "is", "?name",      4]], [{}]),
      when_was_young: λ.curry(inq.q, fb, [["prophet", "is", "young", "?txn"]], [{}]),
      who_is_current: λ.curry(inq.q, fb, [["prophet", "is", "?name"        ]], [{}]),
      names_in_order: λ.curry(inq.q, fb, [["prophet", "is", "?name", "?txn"]], [{}])
    }, function(err, r){
      t.deepEqual(_.pluck(r.first, "?name"), ["smith"]);
      t.deepEqual(_.pluck(r.third, "?name"), ["taylor"]);
      t.deepEqual(_.pluck(r.when_was_young, "?txn"), [3]);
      t.deepEqual(_.pluck(r.who_is_current, "?name"), ["monson"]);
      t.deepEqual(_.pluck(_.sortBy(r.names_in_order, "?txn"), "?name"), prophets);
      t.end(err);
    });
  });
});

test("queries using fb_versions", function(t){
  setupProphetDataset(function(err, fb_versions){
    if(err) return t.end(err);
    λ.map(fb_versions, function(fb, callback){
      //run the same query on each version of the db
      inq.q(fb, [["prophet", "is", "?name"]], [{}], callback);
    }, function(err, r){
      r.map(function(bindings, i){
        t.deepEqual(
          bindings,
          i === 0 ? [] : [{"?name": prophets[i - 1]}]
        );
      });
      t.end(err);
    });
  });
});

test("getEntity", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);
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
      var fb = transactor.connection.snap();
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
      t.deepEqual(_.pluck(r.all_entities, "?entity").sort(), ['01', '02', '_txid1', '_txid2', 'axl', 'brick', 'frankie', 'janet', 'mike', 'rusty', 'sue']);
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
      var fb = transactor.connection.snap();
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

test("multi-valued attributes", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    λ.series([
      λ.curry(transactor.transact, [["0", "_db/attribute"      , "emails"],
                                    ["0", "_db/type"           , "String"],
                                    ["0", "_db/is-multi-valued", true]], {}),

      λ.curry(transactor.transact, [["me", "emails", "1@email"]], {}),
      λ.curry(transactor.transact, [["me", "emails", "2@email"],
                                    ["me", "emails", "3@email"]], {})
    ], function(err, fb_versions){
      if(err) return t.end(err);
      var fb = transactor.connection.snap();

      λ.concurrent({
        my_emails:    λ.curry(inq.q, fb, [["me", "emails", "?emails"]], [{}]),
        the_first_me: λ.curry(inq.getEntity, fb_versions[1], "me"),
        the_last_me:  λ.curry(inq.getEntity, fb, "me")
      }, function(err, r){
        if(err) return t.end(err);

        t.deepEqual(_.pluck(r.my_emails, "?emails"), ["1@email", "2@email", "3@email"]);
        t.deepEqual(r.the_first_me, {emails: ["1@email"]});
        t.deepEqual(r.the_last_me, {emails: ["1@email", "2@email", "3@email"]});

        t.end();
      });
    });
  });
});

test("attribute type encoding/decoding", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    λ.series([
      λ.curry(transactor.transact, [["s0", "_db/attribute"      , "time"],
                                    ["s0", "_db/type"           , "Date"],
                                    ["s0", "_db/is-multi-valued", true],

                                    ["s1", "_db/attribute"      , "int"],
                                    ["s1", "_db/type"           , "Integer"],

                                    ["s2", "_db/attribute"      , "float"],
                                    ["s2", "_db/type"           , "Number"]], {}),

      λ.curry(transactor.transact, [["1",  "time", new Date(2010, 11, 25)]], {}),
      λ.curry(transactor.transact, [["2",   "int", 123]], {}),
      λ.curry(transactor.transact, [["3", "float", 123.45]], {})
    ], function(err, fb_versions){
      if(err) return t.end(err);
      var fb = transactor.connection.snap();

      t.ok(fb.schema.time["_db/is-multi-valued"] === true, "must also decode db default schema values");

      λ.concurrent({
        time1:    λ.curry(inq.q, fb, [["1", "time", "?val"]], [{}]),
        integer1: λ.curry(inq.q, fb, [["2", "int", "?val"]], [{}]),
        number1:  λ.curry(inq.q, fb, [["3", "float", "?val"]], [{}]),

        //query with variable attribute name
        time2:    λ.curry(inq.q, fb, [["1", "?a", "?val"]], [{}]),
        integer2: λ.curry(inq.q, fb, [["2", "?a", "?val"]], [{}]),
        number2:  λ.curry(inq.q, fb, [["3", "?a", "?val"]], [{}]),

        //query with unknown attribute name
        time3:    λ.curry(inq.q, fb, [["1", "?_", "?val"]], [{}]),
        integer3: λ.curry(inq.q, fb, [["2", "?_", "?val"]], [{}]),
        number3:  λ.curry(inq.q, fb, [["3", "?_", "?val"]], [{}]),
        
        //encode values at query with known attribute name
        time4:    λ.curry(inq.q, fb, [["?e", "time", new Date(2010, 11, 25)]], [{}]),
        integer4: λ.curry(inq.q, fb, [["?e", "int", 123]], [{}]),
        number4:  λ.curry(inq.q, fb, [["?e", "float", 123.45]], [{}]),

        //encode values at query with variable attribute name
        time5:    λ.curry(inq.q, fb, [["?e", "?a", new Date(2010, 11, 25)]], [{}]),
        integer5: λ.curry(inq.q, fb, [["?e", "?a", 123]], [{}]),
        number5:  λ.curry(inq.q, fb, [["?e", "?a", 123.45]], [{}]),

        //encode values at query with unknown attribute name
        time6:    λ.curry(inq.q, fb, [["?e", "?_", new Date(2010, 11, 25)]], [{}]),
        integer6: λ.curry(inq.q, fb, [["?e", "?_", 123]], [{}]),
        number6:  λ.curry(inq.q, fb, [["?e", "?_", 123.45]], [{}])
      }, function(err, r){
        if(err) return t.end(err);

        _.each(r, function(results, key){
          t.equal(results.length, 1, "all these type encode/decode queries should return 1 result");
        });

        t.ok(_.isDate(r.time1[0]['?val']));
        t.ok(_.isDate(r.time2[0]['?val']));
        t.ok(_.isDate(r.time3[0]['?val']));

        t.ok(_.isNumber(r.integer1[0]['?val']));
        t.ok(_.isNumber(r.integer2[0]['?val']));
        t.ok(_.isNumber(r.integer3[0]['?val']));
        t.equal(r.integer1[0]['?val'], 123);
        t.equal(r.integer2[0]['?val'], 123);
        t.equal(r.integer3[0]['?val'], 123);

        t.ok(_.isNumber(r.number1[0]['?val']));
        t.ok(_.isNumber(r.number2[0]['?val']));
        t.ok(_.isNumber(r.number3[0]['?val']));
        t.equal(r.number1[0]['?val'], 123.45);
        t.equal(r.number2[0]['?val'], 123.45);
        t.equal(r.number3[0]['?val'], 123.45);

        t.equal(r.time4[0]['?e'], "1");
        t.equal(r.time5[0]['?e'], "1");
        t.equal(r.time6[0]['?e'], "1");
        t.equal(r.integer4[0]['?e'], "2");
        t.equal(r.integer5[0]['?e'], "2");
        t.equal(r.integer6[0]['?e'], "2");
        t.equal(r.number4[0]['?e'], "3");
        t.equal(r.number5[0]['?e'], "3");
        t.equal(r.number6[0]['?e'], "3");

        t.end();
      });
    });
  });
});

test("delayed join, and join order", function(t){
  var db = level(memdown);
  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    transactor.transact([
      ["0", "_db/attribute", "->"],
      ["0", "_db/type"     , "Entity_ID"],
      ["0", "_db/is-multi-valued", true]
    ], {}, function(err){
      if(err) return t.end(err);
      transactor.transact([
        ["a", "->", "c"],
        ["a", "->", "d"],
        ["a", "->", "e"],

        ["b", "->", "f"],
        ["b", "->", "g"],
        ["b", "->", "h"],

        ["d", "->", "g"]
      ], {}, function(err, fb){
        if(err) return t.end(err);

        λ.concurrent({
          one_row:         λ.curry(inq.q, fb, [["a", "->", "?va"]], [{}]),
          no_join:         λ.curry(inq.q, fb, [["a", "->", "?va"],
                                               ["b", "->", "?vb"]], [{}]),
          da_join:         λ.curry(inq.q, fb, [["a", "->", "?va"],
                                               ["b", "->", "?vb"],
                                               ["?va", "->", "?vb"]], [{}]),
          da_join_reverse: λ.curry(inq.q, fb, [["?va", "->", "?vb"],
                                               ["a", "->", "?va"],
                                               ["b", "->", "?vb"]], [{}]),
          da_join_mix:     λ.curry(inq.q, fb, [["a", "->", "?va"],
                                               ["?va", "->", "?vb"],
                                               ["b", "->", "?vb"]], [{}])
        }, function(err, r){
          if(err) return t.end(err);

          t.equal(r.one_row.length, 3, "should return everthing a points to");
          t.equal(r.no_join.length, 9, "should return every combination of a and b pointers");

          t.deepEqual(r.da_join, [{'?va': 'd', '?vb': 'g'}]);
          t.deepEqual(r.da_join_reverse, r.da_join, "q tuple order shouldn't matter");
          t.deepEqual(r.da_join_mix, r.da_join, "q tuple order shouldn't matter");

          t.end();
        });
      });
    });
  });
});
