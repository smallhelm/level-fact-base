var λ = require('contra');
var test = require('tape');
var level = require('levelup');
var memdown = require('memdown');
var getEntity = require('./getEntity');
var Transactor = require('./transactor');

test("getEntity", function(t){
  var db = level(memdown);
  Transactor(db, function(err, transactor){
    if(err) return t.end(err);
    λ.series([
      λ.curry(transactor.transact, [["01", "_db/attribute", "email"],
                                    ["01", "_db/type"     , "String"],
                                    ["02", "_db/attribute", "name"],
                                    ["02", "_db/type"     , "String"]]),

      λ.curry(transactor.transact, [["u0", "email", "andy@email.com"],
                                    ["u0", "name",  "andy"]]),

      λ.curry(transactor.transact, [["u1", "email", "opie@email.com"],
                                    ["u1", "name",  "opie"]]),

      λ.curry(transactor.transact, [["u0", "email", "new@email.com"]])
    ], function(err){
      if(err) return t.end(err);
      var fb = transactor.connection.snap();
      λ.concurrent({
        u0: λ.curry(getEntity, fb, "u0"),
        u1: λ.curry(getEntity, fb, "u1"),
        u2: λ.curry(getEntity, fb, "u2")
      }, function(err, r){
        t.deepEqual(r.u0, {name: "andy", email: "new@email.com"});
        t.deepEqual(r.u1, {name: "opie", email: "opie@email.com"});
        t.deepEqual(r.u2, null);
        t.end(err);
      });
    });
  });
});
