var util    = require('util'),
    path    = require('path'),
    File    = require('./file').klass;

var MAX_WRITE_TRIES = 10;

var createRandomString = function(string_length) {
  if (string_length == 0) {
    string_length = 6;
  }
  var chars = "0123456789abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXZ";
  var randomstring = '';
  for (var i=0; i<string_length; i++) {
    var rnum = Math.floor(Math.random() * chars.length);
    randomstring += chars.substring(rnum,rnum+1);
  }
  return randomstring;
};

var makePath = function(callback) {
  var self = this;
  var randomString = 'alfred_sync_' + createRandomString(64) + '_' + Date.now();
  var file_path= '/tmp/' + randomString;
  path.exists(file_path, function(exists) {
    if (exists) {
      makePath(callback);
    } else {
      callback(file_path);
    }
  });
};

var TempFile = function(callback) {
};

util.inherits(TempFile, File);

module.exports.open = function(callback) {
  var tf = new TempFile(callback);
  makePath(function(path) {
    File.call(tf, path, callback);
  });
};