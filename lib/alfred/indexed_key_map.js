var sys = require('sys') || require('util');
var memory_index = require('./memory_index');

var KeyMap = require('./key_map').class;

var IndexedKeyMap = function(file_path, callback) {
  var index = this.index = memory_index.open();
  KeyMap.call(this, file_path, callback);
  var self = this;
  this.each_with_pos(function(err, record, positio) {
    if (err) {
      callback(err);
    } else {
      index.put(record.key, )
    }
  });
};

sys.inherits(IndexedKeyMap, KeyMap);

IndexedKeyMap.prototype.put = function(key, value, callback) {
  KeyMap.call(this, key, value, callback);
};