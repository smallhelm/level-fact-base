var _ = require('lodash');
var λ = require('contra');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var Connection = require('./connection');
var Transactor = require('./transactor');

test("Ensure the Connection warms up right", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    if(err) return t.end(err);

    λ.series([
      λ.curry(transactor.transact, [
        ["1", "_db/attribute", "name"],
        ["1", "_db/type"     , "String"],
      ], {}),
      λ.curry(transactor.transact, [
        ["2", "_db/attribute", "birthday"],
        ["2", "_db/type"     , "String"],
      ], {}),
      λ.curry(transactor.transact, [
        ["3", "_db/attribute", "birthday"],
        ["3", "_db/type"     , "Date"],
      ], {})
    ], function(err){
      if(err) return t.end(err);

      Connection(db, {}, function(err, conn){
        if(err) return t.end(err);

        t.equals(conn.snap().txn, 3);
        t.deepEquals(_.object(_.filter(_.pairs(conn.snap().schema), function(p){
          return p[0][0] !== "_";
        })), {
          "name": {
            "_db/attribute": "name",
            "_db/type": "String"
          },
          "birthday": {
            "_db/attribute": "birthday",
            "_db/type": "Date"
          }
        });
        t.end();
      });
    });
  });
});
