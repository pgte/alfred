var util = require('util');

var AlfredCheckError = module.exports = function(message, error) {
  this._ALFRED_CHECK_ERROR = error;
  Error.call(this, message);
};
util.inherits(AlfredCheckError, Error);
