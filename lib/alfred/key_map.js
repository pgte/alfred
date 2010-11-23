var collection = require('./collection'),
    sys        = require('sys') || require('util');

var KeyMap = module.exports.class = function(file_path, callback) {
  this.file_path = file_path;
  var self = this;
  collection.open(file_path, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      self.collection = coll;
      callback(null, self);
    }
  });
  
};

module.exports.open = function(file_path, callback) {
  return new KeyMap(file_path, callback);
};

KeyMap.prototype.put = function(key, value, callback) {
  this.collection.write({
    k: key,
    v: value,
    created: (new Date()).getTime()
  },
  callback);
};

KeyMap.prototype.get = function(key, callback) {
  var value = null;
  this.collection.filter(function(record) {
    return record.k === key;
  }, function(error, record) {
    if (error) {
      callback(error);
    } else {
      if (record !== null) {
        value = record.v;
      } else {
        callback(null, value);
      }
    }
  });
};

KeyMap.prototype.get_at_pos = function(pos, length, callback) {
  if (!length) {
    callback(new Error("invalid length"));
  }
  
  this.collection.fetch(pos, length, function(err, record) {
    if (err) {
      callback(err);
    } else {
      if (record === null) {
        callback(null, null);
      } else {
        callback(null, record.k, record.v);
      }
    }
  });
};

KeyMap.prototype.clear = function(callback) {
  this.collection.clear(callback);
};

KeyMap.prototype.each = function(callback, null_on_end) {
  this.collection.read(function(err, record, pos, length) {
    if (err) {
      callback(err);
    } else {
      if (record) {
        callback(null, record.k, record.v, pos, length);
      } else {
        if (null_on_end) {
          callback(null, null);
        }
      }
    }
  }, null_on_end);
};

KeyMap.prototype.end = function(callback) {
  this.collection.end(callback);
};