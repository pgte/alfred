var util            = require('util'),
    cache          = require('./util/cache'),
    options_merger = require('./util/options_merger');

var IndexedKeyMap = require('./indexed_key_map').klass;

var default_options = {
  cache_slots: 1000
};

var CachedKeyMap = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  this.options = options_merger.merge(default_options, options);
  
  IndexedKeyMap.call(this, file_path, this.options, function(err) {
    if (err) {
      callback(err);
    } else {
      self.cache = cache.open(self.options.cache_slots);
      IndexedKeyMap.prototype.count.call(self, function(err, count) {
        if (count == 0) {
          callback(null, self);
          return;
        }
        var done = 0;
        IndexedKeyMap.prototype.eachInIndex.call(self, function(err, key, value) {
          if (err) { callback(err); return; }
          if (key && value) {
            self.cache.put(key, value);
          }
          done ++;
          if (done == count) {
            callback(null, self);
          }
        });
      });
    }
  });
};

util.inherits(CachedKeyMap, IndexedKeyMap);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  return new CachedKeyMap(file_path, options, callback);
};

CachedKeyMap.prototype.put = function(key, value, callback, secret) {
  var self = this;
  
  IndexedKeyMap.prototype.put.call(this, key, value, function(err, position, length) {
    if (err) {
      callback(err);
    } else {
      self.cache.put(key, value);
      callback(null, position, length);
    }
  }, secret);
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