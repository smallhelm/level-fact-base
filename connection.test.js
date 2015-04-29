var _ = require('lodash');
var λ = require('contra');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var Connection = require('./connection');
var Transactor = require('./transactor');

test("Ensure the Connection warms up right", function(t){
  var db = level(memdown);

  var fbStateEquals = function(fb, txn, user_schema){
    t.equals(fb.txn, txn);
    t.deepEquals(_.object(_.filter(_.pairs(fb.schema), function(p){
      return p[0][0] !== "_";
    })), user_schema);
  };

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

        conn.asOf(2, function(err, fb_2){
          if(err) return t.end(err);
          conn.asOf(1, function(err, fb_1){
            if(err) return t.end(err);


            fbStateEquals(fb_1, 1, {
              "name": {
                "_db/attribute": "name",
                "_db/type": "String"
              }
            });

            fbStateEquals(fb_2, 2, {
              "name": {
                "_db/attribute": "name",
                "_db/type": "String"
              },
              "birthday": {
                "_db/attribute": "birthday",
                "_db/type": "String"
              }
            });

            fbStateEquals(conn.snap(), 3, {
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
  });
});
