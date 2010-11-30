var sys                 = require('sys') || require('util'),
    path                = require('path'),
    fs                  = require('fs'),
    collection          = require('./collection'),
    memory_index        = require('./memory_index'),
    functional_index    = require('./functional_index'),
    compact_coordinator = require('./compact_coordinator');
    

var KeyMap = require('./key_map').klass;

var IndexedKeyMap = module.exports.klass = function(file_path, callback) {
  var self = this;
  KeyMap.call(this, file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      var index = self.index = memory_index.open();
      self.each(function(err, key, value, pos, length) {
        if (err) {
          callback(err);
        } else {
          if (key) {
            index.put(key, pos, length, function(err) {
              if (err) {
                callback(err);
              }
            });
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
              
              self.notifyCoordinators('put', [key, value], function(err) {
                if (err) {
                  callback(err);
                }
              });
              
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
        record = KeyMap.prototype.get_at_pos.call(self, index.o, index.l, function(err, key, value) {
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

IndexedKeyMap.prototype.clear = function(callback) {
  this.index.clear();
  KeyMap.prototype.clear.call(this, callback);
};

IndexedKeyMap.prototype.each_in_index = function(callback) {
  this.index.each(function(err, key, value) {
    if (err) {
      callback(err);
    } else {
      callback(null, key, value);
    }
  });
};

/** Atomic **/

IndexedKeyMap.prototype.atomic = function(key, callback, result_callback) {
  var self = this;
  this.index.atomic(key, null, function(err, secret, record, next) {
    var new_value = callback(err, record);
    self.put(key, new_value, function(err) {
      next();
      if (err) {
        result_callback(err);
      } else {
        result_callback(null);
      }
    }, secret);
  });
};

IndexedKeyMap.prototype.compact = function(callback) {
  var self = this;
  var compact_coordinator_name = 'compact';
  if (this.hasCoordinator(compact_coordinator_name)) {
    callback(new Error('Compacting is already in progress'));
  } else {
    var coordinator = compact_coordinator.create(this);
    this.addCoordinator(compact_coordinator_name, coordinator);
    coordinator.start(function(err) {
      self.removeCoordinator(compact_coordinator_name);
      callback(err);
    });
  }
};