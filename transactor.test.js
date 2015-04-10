var _ = require('lodash');
var test = require('tape');
var async = require('async');
var level = require('levelup');
var memdown = require('memdown');
var genRandomString = require('./utils/genRandomString');

var Transactor = require('./transactor');

test("ensure transact persists stuff to the db", function(t){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    transactor.transact([
      ["0001", "name", "bob"],
      ["0001", "age",   "34"],
      ["0002", "name", "jim"],
      ["0002", "age",   "23"]
    ], {
      user_id: "0001"
    }, function(err){
      if(err){
        return t.end(err);
      }
      var all_data = [];
      db.readStream().on('data', function(data){
        all_data.push(data);
      }).on('close', function(){
        t.equals(all_data.length, 24);
        t.end();
      });
    });
  });
});
