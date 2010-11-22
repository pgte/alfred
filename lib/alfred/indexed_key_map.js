var sys = require('sys') || require('util');
var memory_index = require('./memory_index');

var KeyMap = require('./key_map').class;

var IndexedKeyMap = function(file_path, callback) {
  var self = this;
  KeyMap.call(this, file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      var index = self.index = memory_index.open();
      self.each_with_pos(function(err, key, value, pos, length) {
        if (err) {
          callback(err);
        } else {
          if (key) {
            index.put(key, pos, length);
          } else {
            // end
            callback(null, self);
          }
        }
      }, true);
    }
  });
};

sys.inherits(IndexedKeyMap, KeyMap);

module.exports.open = function(file_path, callback) {
  return new IndexedKeyMap(file_path, callback);
};

IndexedKeyMap.prototype.put = function(key, value, callback) {
  var self = this;
  KeyMap.prototype.put.call(this, key, value, function(err, position, length) {
    self.index.put(key, position, length);
  });
};

IndexedKeyMap.prototype.get = function(key, callback) {
  var index = this.index.get(key);
  var record;
  if (index) {
    record = KeyMap.prototype.get_at_pos.call(this, index.offset, index.length, function(err, record) {
      callback(null, record);
    });
  } else {
    callback(null, null);
  }
};

IndexedKeyMap.prototype.clear = function(callback) {
  this.index.clear();
  KeyMap.prototype.clear.call(this, callback);
}
