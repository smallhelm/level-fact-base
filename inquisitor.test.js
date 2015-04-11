var _ = require('lodash');
var test = require('tape');
var async = require('async');
var level = require('levelup');
var memdown = require('memdown');
var genRandomString = require('./utils/genRandomString');

var Inquisitor = require('./inquisitor');
var Transactor = require('./transactor');

var setupMiddleDataset = function(callback){
  var db = level(memdown);

  Transactor(db, {}, function(err, transactor){
    if(err){
      return callback(err);
    }
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
      if(err){
        return callback(err);
      }
      callback(null, Inquisitor(db));
    });
  });
};

test("basic qTuple stuff", function(t){
  setupMiddleDataset(function(err, inq){
    if(err){
      return t.end(err);
    }
    async.parallel({
      axl_mother:           async.apply(inq.qTuple, ["axl",       "mother", "?mother"], {}),
      axl_relation_to_mike: async.apply(inq.qTuple, ["axl",    "?relation", "mike"], {}),
      mikes_children:       async.apply(inq.qTuple, ["?children", "father", "?father"], {"?father": "mike"}),
      axl_has_no_children:  async.apply(inq.qTuple, ["?children", "father", "axl"], {})
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
    if(err){
      return t.end(err);
    }
    async.parallel({
      husbands_and_wifes:   async.apply(inq.q, [["?child", "mother", "?wife"],
                                                ["?child", "father", "?husband"]], [{}]),

      sue_grandfathers:     async.apply(inq.q, [[    "sue", "father", "?father"],
                                                [    "sue", "mother", "?mother"],
                                                ["?mother", "father", "?grandpa1"],
                                                ["?father", "father", "?grandpa2"]], [{}]),

      sue_siblings:         async.apply(inq.q, [[    "?sue", "mother", "?mother"],
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
