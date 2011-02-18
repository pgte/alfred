var util = require('util');

var AlfredCheckError = module.exports = function(message, error) {
  Error.apply(this, arguments);
  this.message = message;
  this._ALFRED_CHECK_ERROR = error;
  Error.captureStackTrace(this);
};

util.inherits(AlfredCheckError, Error);