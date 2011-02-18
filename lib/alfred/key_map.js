var collection       = require('./collection'),
    ordered_index    = require('./indexes/ordered_index'),
    unordered_index  = require('./indexes/unordered_index'),
    options_merger   = require('./util/options_merger'),
    util             = require('util'),
    EventEmitter     = require('events').EventEmitter;

var default_index_options = {
  ordered: true
};

var KeyMap = module.exports.klass = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  
  this.file_path = file_path;
  this.indexes = {};
  this.coordinators = {};
  var self = this;
  collection.open(file_path, options, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      // listen to errors and emit them
      coll.on('error', function(err) {
        self.emit('error', err);
      });
      
      coll.on('before_flush', function() {
        self.emit('before_flush');
      });
      
      coll.on('after_flush', function() {
        self.emit('after_flush');
      });
      
      self.collection = coll;
      
      // we're done
      callback(null, self);
    }
  });
  
};

util.inherits(KeyMap, EventEmitter);

module.exports.open = function(file_path, callback) {
  var opts;
  
  if (arguments.length > 2 && arguments[2]) {
    opts = arguments[1];
    callback = arguments[2];
  }
  
  return new KeyMap(file_path, opts, callback);
};

KeyMap.prototype.put = function(key, value, callback, collection) {
  if (!collection) {
    collection = this.collection;
  }
  var self = this;
  var new_obj = {
    k: key,
    v: value,
    t: Date.now()
  };
  collection.write(new_obj, function(err, pos, length) {
    if (err) {
      callback(err);
    } else {
      for(var index_index in self.indexes) {
        if (self.indexes.hasOwnProperty(index_index)) {
          self.indexes[index_index].put(key, value, pos, length);
        }
      }
      
      if (self.indexes_temp) {
        for(var index_index in self.indexes_temp) {
          if (self.indexes.hasOwnProperty(index_index)) {
            self.indexes[index_index].put(key, value, pos, length);
          }
        }
      }
      
      self.emit('put', key, value);
      callback(null, pos, length);
    }
  });
};

KeyMap.prototype.putAtPos = function(key, value, pos, callback, collection) {
  if (!collection) {
    collection = this.collection;
  }
  var self = this;
  var new_obj = {
    k: key,
    v: value,
    created: (new Date()).getTime()
  };
  collection.writeAtPos(new_obj, pos, function(err, pos, length) {
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
      if (record != null) {
        value = record.v;
      }
      if (record !== null || value === null) {
        callback(null, value);
      }
    }
  });
};

KeyMap.prototype.destroy = function(key, callback) {
  this.put(key, null, callback);
};

KeyMap.prototype.getAtPos = function(pos, length, callback) {
  if (!length) {
    callback(new Error("invalid length: " + length));
    return;
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
  var ended = false;
  this.collection.read(function(err, record, pos, length) {
    if (err) {
      callback(err);
    } else {
      if (record) {
        if (record.k) {
          callback(null, record.k, record.v, pos, length);
        }
      } else {
        if (null_on_end && !ended) {
          ended = true;
          callback(null, null);
        }
      }
    }
  }, null_on_end);
};

KeyMap.prototype.end = function(callback) {
  var self = this;
  if (this.ending) {
    callback(new Error(this.file_path + ' is already ending'));
    return;
  }
  this.ending = true;
  this.collection.end(function(err) {
    delete self.ending;
    callback(err);
  });
};

KeyMap.prototype.endSync = function() {
  var self = this;
  if (this.ending) {
    callback(new Error(this.file_path + ' is already ending'));
    return;
  }
  return this.collection.endSync();
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

/* key_map.addIndex(name [, options], transform_function, callback)*/
KeyMap.prototype.addIndex = function(name, transform_function, callback) {
  var options;
  if (arguments.length == 4 && !!arguments[3]) {
    options = arguments[1];
    transform_function = arguments[2];
    callback = arguments[3];
  }
  
  var merged_options = options_merger.merge(default_index_options, options);
  var index, index_constructor;
  
  index_constructor = merged_options.ordered ?
    ordered_index :
    unordered_index;

  index = index_constructor.open(transform_function, merged_options);
  this.indexes[name] = index;
  
  this.populateIndex(index, callback);
};

KeyMap.prototype.populateIndex = function(index, callback) {
  this.each(function(err, key, value, pos, length) {
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
}

KeyMap.prototype.dropIndex = function(name, callback) {
  delete this.indexes[name];
  callback(null);
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
    self.getAtPos(pos, length, function(err, key, value) {
      if (err) {
        callback(err);
      } else {
        callback(null, key, value);
      }
    });
  }, null_on_not_found);
};

KeyMap.prototype.range = function(index, start, end, callback) {
  var use_index = index;
  var self = this;
  if (typeof(index) == 'string') {
    use_index = this.indexes[index];
    if (!use_index) {
      callback(new Error("Index with name '" + index + "' not found"));
      return;
    }
  }
  if (!use_index.range) {
    callback(new Error('Index \'' + index + '\' does not have range'));
  } else {
    use_index.range(start, end, function(key, pos, length) {
      self.getAtPos(pos, length, function(err, key, value) {
        if (err) {
          callback(err);
        } else {
          callback(null, key, value);
        }
      });
    });
  }
  
};

KeyMap.prototype.countFilter = function(index, filter_function, callback) {
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

KeyMap.prototype.indexMatch = function(index, value, callback) {
  var self = this;
  
  var use_index = index;
  if (typeof(index) == 'string') {
    use_index = this.indexes[index];
    if (!use_index) {
      callback(new Error("Index with name '" + index + "' not found"));
      return;
    }
  }
  use_index.match(value, function(key, pos, length) {
    if (key !== null) {
      self.getAtPos(pos, length, function(err, key, value) {
        if (err) {
          callback(err);
        } else {
          callback(null, key, value);
        }
      });
    } else {
      callback(null, null, null);
    }
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
            self.emit('error', err);
          }
        });
      })(index);
    }
  }
};

/* Streams */

KeyMap.prototype.startStream = function(filter_function, callback) {
  var handler = function(key, value) {
    if (filter_function(key, value)) {
      callback(key, value);
    };
  };
  
  this.on('put', handler);
  return handler;
};

KeyMap.prototype.stopStream = function(handler) {
  this.removeListener('put', handler);
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