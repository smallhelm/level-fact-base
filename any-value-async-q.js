var _ = require('lodash');
var λ = require('contra');

module.exports = function(worker){

  var q_data = {};

  var q = λ.queue(function(q_id, callback){
    worker(q_data[q_id], callback);
    delete q_data[q_id];
  });

  return {
    push: function(data, callback){
      var q_id = _.uniqueId();
      q_data[q_id] = data;
      q.unshift(q_id, callback);//why unshift??? don't know
    }
  };
};
