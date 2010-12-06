var collection       = require('./collection'),
    unordered_index = require('./indexes/unordered_index'),
    sys              = require('sys') || require('util'),
    path             = require('path'),
    EventEmitter     = require('events').EventEmitter;

var KeyMap = module.exports.klass = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    callback = arguments[2];
    options = arguments[1];
  }
  
  this.file_path = file_path;
  this.indexes = {};
  this.coordinators = {};
  var self = this;
  collection.open(file_path, options, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      self.collection = coll;
      callback(null, self);
    }
  });
  
};

sys.inherits(KeyMap, EventEmitter);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    callback = arguments[2];
    options = arguments[1];
  }
  return new KeyMap(file_path, options, callback);
};

KeyMap.prototype.put = function(key, value, callback, collection) {
  if (!collection) {
    collection = this.collection;
  }
  var self = this;
  var new_obj = {
    k: key,
    v: value,
    created: (new Date()).getTime()
  };
  collection.write(new_obj, function(err, pos, length) {
    if (err) {
      callback(err);
    } else {
      for(var index_index in self.indexes) {
        if (self.indexes.hasOwnProperty(index_index)) {
          var index = self.indexes[index_index];
          index.put(key, value, pos, length);
        }
      }
      
      if (self.indexes_temp) {
        for(var index_index in self.indexes_temp) {
          if (self.indexes.hasOwnProperty(index_index)) {
            var index = self.indexes[index_index];
            index.put(key, value, pos, length);
          }
        }
      }
      
      callback(null, pos, length);
    }
  });
};

KeyMap.prototype.put_at_pos = function(key, value, pos, callback, collection) {
  if (!collection) {
    collection = this.collection;
  }
  var self = this;
  var new_obj = {
    k: key,
    v: value,
    created: (new Date()).getTime()
  };
  collection.write_at_pos(new_obj, pos, function(err, pos, length) {
    if (err) {
      callback(err);
    } else {
      callback(null, pos, length);
    }
  });
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

KeyMap.prototype.rename = function(new_name, callback) {
  this.collection.rename(new_name, function(err) {
    if (err) {
      callback(err);
    } else {
      this.file_path = new_name;
      callback(null);
    }
  });
};

/** Indexes **/

KeyMap.prototype.addIndex = function(name, transform_function, callback) {
  var index = this.indexes[name] = unordered_index.open(transform_function);
  this.each(function(err, key, value, pos, length) {
    if (err) {
      callback(err);
    }
    if (key) {
      index.put(key, value, pos, length);
    } else {
      callback(null, index);
    }
  }, true);
};

KeyMap.prototype.getIndex = function(name) {
  return this.indexes[name];
};

KeyMap.prototype.filter = function(index, filter_function, callback, null_on_not_found) {
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
    self.get_at_pos(pos, length, function(err, key, value) {
      if (err) {
        callback(err);
      } else {
        callback(null, key, value);
      }
    });
  }, null_on_not_found);
};

KeyMap.prototype.count_filter = function(index, filter_function, callback) {
  var self = this;

  process.nextTick(function() {
    var use_index = index;
    if (typeof(index) == 'string') {
      use_index = self.indexes[index];
      if (!use_index) {
        callback(new Error("Index with name '" + index + "' not found"));
        return;
      }
    }
    use_index.count(filter_function, callback);
  });
};

/** Coordinators **/

KeyMap.prototype.addCoordinator = function(name, coordinator) {
  this.coordinators[name] = coordinator;
};

KeyMap.prototype.removeCoordinator = function(name) {
  delete this.coordinators[name];
};

KeyMap.prototype.hasCoordinator = function(name) {
  return this.coordinators.hasOwnProperty(name);
};

KeyMap.prototype.notifyCoordinators = function(name, args) {
  var self = this;
  for(var index in this.coordinators) {
    if (this.coordinators.hasOwnProperty(index)) {
      (function(index) {
        self.coordinators[index].notify(name, args, function(err) {
          if (err) {
            throw err;
          }
        });
      })(index);
    }
  }
};

/* Error handling */

KeyMap.prototype.uncaughtErrorCallback = function(err) {
  if (err) {
    if (this.listeners('error').length > 0) {
      this.emit(error, err);
    } else {
      throw err
    }
  }
};