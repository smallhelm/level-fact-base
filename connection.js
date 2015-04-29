var λ = require('contra');
var HashIndex = require('level-hash-index');

var getLatestedTxn = function(db, callback){
  var stream = db.createReadStream({
    keys: true,
    values: false,
    reverse: true,
    gte: 'teavo!\x00',
    lte: 'teavo!\xFF',
  }).on('data', function(data){
    var txn = parseInt(data.split('!')[1], 36);
    callback(null, txn);
    stream.destroy();
  }).on('error', function(err){
    callback(err);
  }).on('end', function(){
    callback(null, 0);
  });
};

module.exports = function(db, options, callback){
  options = options || {};

  var hindex = options.hindex || HashIndex(db);

  λ.concurrent({
    latest_transaction_n: function(callback){
      getLatestedTxn(db, callback);
    }
  }, function(err, data){
    if(err) return callback(err);

    var latest_transaction_n = data.latest_transaction_n;

    callback(null, {
      snap: function(){
        return {
          txn: latest_transaction_n,

          db: db,
          hindex: hindex
        };
      }
    });
  });
};
