var util                = require('util'),
    key_index           = require('./indexes/key_index'),
    compact_coordinator = require('./util/compact_coordinator'),
    assert              = require('assert');
    

var KeyMap = require('./key_map').klass;

var IndexedKeyMap = module.exports.klass = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  
  this.options = options;
  
  KeyMap.call(this, file_path, options, function(err) {
    if (err) {
      callback(err);
    } else {
      var index = self.index = key_index.open();
      self.each(function(err, key, value, pos, length) {
        if (err) {
          callback(err);
        } else {
          if (key) {
            index.put(key, pos, length, self.uncaughtErrorCallback);
          } else {
            // end
            callback(null, self);
          }
        }
      }, true);
    }
  });
};

util.inherits(IndexedKeyMap, KeyMap);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  return new IndexedKeyMap(file_path, options, callback);
};

IndexedKeyMap.prototype.put = function(key, value, callback) {
  var self = this;
  var secret = arguments[3];
  self.index.atomic(key, secret, function(err, secret, record, next) {
    next();
    if (err) {
      callback(err);
    } else {
      KeyMap.prototype.put.call(self, key, value, function(err, position, length) {
        if (err) {
          callback(err);
        } else {
          self.index.put(key, position, length, function(err) {
            if (err) {
              callback(err);
            } else {
              callback(null, position, length);
              
              self.notifyCoordinators('put', [key, value, function(err) {
                if (err) {
                  self.emit('error', err);
                }
              }]);
              
            }
          }, secret);
        }
      });
    }
  });
};

IndexedKeyMap.prototype.get = function(key, callback) {
  var self = this;
  this.index.get(key, function(err, index) {
    if (err) {
      callback(err);
    } else {
      if (index) {
        record = KeyMap.prototype.getAtPos.call(self, index.o, index.l, function(err, key, value) {
          if (err) {
            callback(err);
          } else {
            callback(null, value);
          }
        });
      } else {
        callback(null, null);
      }
    }
  });
};

IndexedKeyMap.prototype.count = function(callback) {
  var self = this;
  process.nextTick(function() {
    var count = self.index.count();
    callback(null, count);
  });
  
};

IndexedKeyMap.prototype.size = function(callback) {
  callback(null, this.index.size());
};

IndexedKeyMap.prototype.clear = function(callback) {
  this.index.clear();
  KeyMap.prototype.clear.call(this, callback);
};

IndexedKeyMap.prototype.eachInIndex = function(callback, null_on_end) {
  var self = this;
  var count = this.index.count();
  var counted = 0;
  if (count == 0) {
    callback(null, null);
    return;
  }
  this.index.each(function(err, key, pos, length) {
    if (err) {
      callback(err);
    } else {
      self.getAtPos(pos, length, function(err, key, value) {
        callback(null, key, value, pos, length);
        counted ++;
        if (null_on_end && counted == count) {
          callback(null, null);
        }
      });
    }
  });
};

IndexedKeyMap.prototype.scan = IndexedKeyMap.prototype.eachInIndex;

/** Atomic **/

IndexedKeyMap.prototype.atomic = function(key, callback, result_callback, secret) {
  var self = this;
  this.index.atomic(key, secret, function(err, secret, record, next) {
    if (err) {
      callback(err);
      return;
    }
    var evalAndSet = function(value) {
      var new_value = callback(null, value);
      if (new_value === null) { return; }
      self.put(key, new_value, function(err) {
        next();
        if (err) {
          result_callback(err);
        } else {
          result_callback(null);
        }
      }, secret);
    }
    if (record === null) {
      evalAndSet(null);
    } else {
      self.getAtPos(record.o, record.l, function(err, retKey, value) {
        if (err) { result_callback(err); return; }
        assert.equal(key, retKey);
        evalAndSet(value);
      });
    }
  });
};

IndexedKeyMap.prototype.compact = function(callback) {
  var self = this;
  var compact_coordinator_name = 'compact';
  if (this.hasCoordinator(compact_coordinator_name)) {
    callback(new Error('Compacting is already in progress'));
  } else {
    self.emit('before_compact');
    var coordinator = compact_coordinator.create(this, this.options);
    this.addCoordinator(compact_coordinator_name, coordinator);
    coordinator.start(function(err) {
      if (err) {
        callback(err)
      } else {
        self.removeCoordinator(compact_coordinator_name);
        callback(null);
        process.nextTick(function() {
          self.emit('after_compact');
        });
      }
    });
  }
};

/* Functional index redux */

KeyMap.prototype.populateIndex = function(index, callback) {
  this.eachInIndex(function(err, key, value, pos, length) {
    if (err) {
      callback(err);
    }
    if (key) {
      index.put(key, value, pos, length);
    } else {
      // last one
      callback(null, index);
    }
  }, true);
};