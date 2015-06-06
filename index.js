var inq = require('./inquisitor');
var Transactor = require('./transactor');

module.exports = function(db, options, onStartup){
  if(arguments.length === 2){
    onStartup = options;
    options = {};
  }
  Transactor(db, options, function(err, transactor){
    if(err) return onStartup(err);

    onStartup(null, {
      transact: transactor.transact,

      connection: transactor.connection,
      snap: transactor.connection.snap,
      asOf: transactor.connection.asOf,

      q: inq.q,
      qTuple: inq.qTuple,
      getEntity: inq.getEntity
    });
  });
};
