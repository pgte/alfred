var sys = require('sys') || require('util');
var memory_index = require('./memory_index');
var cache = require('./cache');

var IndexedKeyMap = require('./indexed_key_map').klass;

var CachedKeyMap = function(file_path, callback) {
  var self = this;
  IndexedKeyMap.call(this, file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      self.cache = cache.open(10000);
      IndexedKeyMap.prototype.each_in_index.call(self, function(err, key, value) {
        self.cache.put(key, value);
      });
      callback(null, self);
    }
  });
};

sys.inherits(CachedKeyMap, IndexedKeyMap);

module.exports.open = function(file_path, callback) {
  return new CachedKeyMap(file_path, callback);
};

CachedKeyMap.prototype.put = function(key, value, callback) {
  var self = this;
  this.cache.put(key, value);
  
  IndexedKeyMap.prototype.put.call(this, key, value, function(err, position, length) {
    if (err) {
      callback(err);
    } else {
      callback(null, position, length);
    }
  });
};

CachedKeyMap.prototype.get = function(key, callback) {
  var self = this;
  var value = this.cache.get(key);
  if (value) {
    callback(null, value);
    return;
  }
  
  IndexedKeyMap.prototype.get.call(this, key, function(err, value) {
    if (err) {
      callback(err);
    } else {
      //self.cache.put(key, value);
      callback(null, value);
    }
  });
};

CachedKeyMap.prototype.clear = function(callback) {
  this.cache.clear();
  IndexedKeyMap.prototype.clear.call(this, callback);
};

CachedKeyMap.prototype.filter = function(index, filter_function, callback, null_on_not_found) {
  var use_index = index;
  var self = this;
  if (typeof(index) == 'string') {
    use_index = this.indexes[index];
    if (!use_index) {
      callback(new Error("Index with name '" + index + "' not found"));
      return;
    }
  }
  use_index.filter(filter_function, function(key, pos, length) {
    if (!key) {
      // nothing was found
      callback(null, null);
      return;
    }
    self.get(key, function(err, value) {
      if (err) {
        callback(err);
      } else {
        callback(null, key, value);
      }
    });
  }, null_on_not_found);
};