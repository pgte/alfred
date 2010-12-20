var master = require('./master');

module.exports.start = function(database, options, callback) {
  if (options.master) {
    return master.start(database, options, callback);
  }
};