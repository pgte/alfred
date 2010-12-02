var sys            = require('sys') || require('util'),
    memory_index   = require('./memory_index');
    cache          = require('./cache'),
    options_merger = require('./options_merger');

var IndexedKeyMap = require('./indexed_key_map').klass;

var default_options = {
  cache_slots: 1000
};

var CachedKeyMap = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    callback = arguments[2];
    options = arguments[1];
  }
  this.options = options_merger.merge(default_options, options);
  
  IndexedKeyMap.call(this, file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      self.cache = cache.open(self.options.cache_slots);
      IndexedKeyMap.prototype.each_in_index.call(self, function(err, key, value) {
        self.cache.put(key, value);
      });
      callback(null, self);
    }
  });
};

sys.inherits(CachedKeyMap, IndexedKeyMap);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    callback = arguments[2];
    options = arguments[1];
  }
  return new CachedKeyMap(file_path, options, callback);
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