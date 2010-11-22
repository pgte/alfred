var collection = require('./collection');

var KeyMap = function() {
};

(require('sys') || require('util')).inherits(KeyMap, require('events').EventEmitter);

module.exports.open = function(file_path, callback) {
  var key_map = new KeyMap();
  collection.open(file_path, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      key_map.collection = coll;
      callback(null, key_map);
    }
  });
};

KeyMap.prototype.put = function(key, value, callback) {
  this.collection.write({
    key: key,
    value: value,
    created: (new Date()).getTime()
  },
  callback);
};

KeyMap.prototype.get = function(key, callback) {
  var value = null;
  this.collection.filter(function(record) {
    return record.key === key;
  }, function(error, record) {
    if (error) {
      callback(error);
      return;
    }
    if (record === null) {
      callback(null, value);
    } else {
      value = record.value;
    }
  });
};

KeyMap.prototype.clear = function(callback) {
  this.collection.clear(callback);
};