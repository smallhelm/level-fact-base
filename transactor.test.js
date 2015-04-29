var _ = require('lodash');
var λ = require('contra');
var inq = require('./inquisitor');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var HashIndex = require('level-hash-index');
var Transactor = require('./transactor');
var genRandomString = require('./utils/genRandomString');

test("ensure schema is loaded on transactor startup", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor1){
    if(err) return t.end(err);

    transactor1.transact([
      ["sky", "color", "blue"]
    ], {}, function(err){
      t.ok(err);
      t.equals(err.toString(), "Error: Attribute not found in schema: color");

      transactor1.transact([
        ["01", "_db/attribute", "color"],
        ["01", "_db/type"     , "String"]
      ], {}, function(err){
        if(err) return t.end(err);

        Transactor(db, {}, function(err, transactor2){
          if(err) return t.end(err);
          transactor2.transact([
            ["sky", "color", "blue"]
          ], {}, function(err){
            t.end(err);
          });
        });
      });
    });
  });
});

test("ensure schema is updated as facts are recorded", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    transactor.transact([
      ["sky", "color", "blue"]
    ], {}, function(err){
      t.ok(err);
      t.equals(err.toString(), "Error: Attribute not found in schema: color");

      transactor.transact([
        ["01", "_db/attribute", "color"],
        ["01", "_db/type"     , "String"]
      ], {}, function(err){
        if(err) return t.end(err);

        transactor.transact([
          ["sky", "color", "blue"]
        ], {}, function(err){
          t.end(err);
        });
      });
    });
  });
});

test("ensure transact persists stuff to the db", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    λ.series([
      λ.curry(transactor.transact, [
        ["01", "_db/attribute", "name"],
        ["01", "_db/type"     , "String"],
        ["02", "_db/attribute", "age"],
        ["02", "_db/type"     , "String"],
        ["03", "_db/attribute", "user_id"],
        ["03", "_db/type"     , "Entity_ID"]
      ], {}),
      λ.curry(transactor.transact, [
        ["0001", "name", "bob"],
        ["0001", "age",   "34"],
        ["0002", "name", "jim"],
        ["0002", "age",   "23"]
      ], {user_id: "0001"})
    ], function(err){
      if(err) return t.end(err);

      var all_data = [];
      db.readStream().on('data', function(data){
        all_data.push(data);
      }).on('close', function(){
        t.equals(all_data.length, 60);
        t.end();
      });
    });
  });
});

test("ensure transactor warms up with the latest transaction id", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    λ.series([
      λ.curry(transactor.transact, [
        ["01", "_db/attribute", "is"],
        ["01", "_db/type"     , "String"],
      ], {}),
      λ.curry(transactor.transact, [["bob", "is", "cool"]], {}),
      λ.curry(transactor.transact, [["bob", "is", "NOT cool"]], {}),
      λ.curry(transactor.transact, [["bob", "is", "cool"]], {})
    ], function(err){
      if(err) return t.end(err);

      var fb = transactor.connection.snap();
      inq.q(fb, [["?_", "?_", "?_", "?txn"]], [{}], function(err, results){
        if(err) return t.end(err);

        var txns = _.unique(_.pluck(results, "?txn")).sort();
        t.deepEqual(txns, [1, 2, 3, 4]);

        //warm up a new transactor to see where it picks up
        Transactor(db, {}, function(err, transactor2){
          if(err) return t.end(err);

          transactor2.transact([["bob", "is", "NOT cool"]], {}, function(err, fb2){
            if(err) return t.end(err);

            inq.q(fb2, [["?_", "?_", "?_", "?txn"]], [{}], function(err, results){
              var txns = _.unique(_.pluck(results, "?txn")).sort();
              t.deepEqual(txns, [1, 2, 3, 4, 5]);
              t.end(err);
            });
          });
        });
      });
    });
  });
});
