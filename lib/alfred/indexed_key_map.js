var sys              = require('sys') || require('util'),
    path             = require('path'),
    fs               = require('fs'),
    collection       = require('./collection');
    memory_index     = require('./memory_index');
    functional_index = require('./functional_index');

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
              
              if (self.collection_temp) { // If we are in the middle of a compactation
                KeyMap.prototype.put.call(self, key, value, function(err, position, length) {
                  if (err) {
                    self.collection_temp_callback(err);
                  } else {
                    self.index_temp.put(key, position, length, function(err) {
                      if (err) {
                        self.collection_temp_callback(err);
                      }
                    });
                  }
                }, self.collection_temp);
              }
              
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

/** Copy into collection **/

IndexedKeyMap.prototype.copyIntoCollectionAndIndexes = function(collection, indexes, callback) {
  
  var self = this;
  
  this.index_temp = this.index.clone();
  var index_temp_temp = this.index_temp.clone();
  
  var count = this.index_temp.count();
  var copied_count = 0;
  var current_pos = 0;
  index_temp_temp.each(function(err, key, rec) {
    self.index_temp.atomic(key, null, function(err, secret, other_rec, next) {
      if (err) {
        callback(err);
        return
      }
      self.get_at_pos(rec.o, rec.l, function(err, key, value) {
        if (err) {
          callback(err);
          return;
        }
        var this_pos = current_pos;
        current_pos += rec.l;
        self.put_at_pos(key, value, this_pos, function(err, position, length) {
          if (err) {
            callback(err);
          } else {
            self.index_temp.put(key, position, length, function(err) {
              if (err) {
                callback(err);
              } else {

                next();
                
                var index;
                for(var index_index in indexes) {
                  if (indexes.hasOwnProperty(index_index)) {
                    index = indexes[index_index];
                    index.put(key, value, position, length);
                  }
                }

                copied_count ++;
                if (copied_count == count) {
                  // Done
                  callback(null);
                }
              }
            }, secret);
          }
        }, collection);
      });
    });
  });
};

/** Compact **/

IndexedKeyMap.prototype.compact = function(callback) {
  var self = this;
  
  if (this.compact_in_progress) {
    callback(new Error('Compacting is already in progress'));
  } else {
    this.compact_in_progress = true;
    var temp_collection_path = self.file_path + '.temp';
    path.exists(temp_collection_path, function(exists) {
      
      var done_removing = function() {
        collection.open(temp_collection_path, function(err, coll) {
          if (err) {
            callback(err);
          } else {
            self.collection_temp_callback = callback;
            self.collection_temp = coll;
            self.size(function(err, size) {
              if (err) {
                callback(err);
              } else {
                
                self.indexes_temp = {};
                var index, cloned_index;
                for(var index_index in self.indexes) {
                  if (self.indexes.hasOwnProperty(index_index)) {
                    index = self.indexes[index_index];
                    cloned_index = new functional_index.open(index.transformFunction());
                    self.indexes_temp[index_index] = cloned_index;
                  }
                }
                
                self.collection_temp.position(size);
                self.copyIntoCollectionAndIndexes(self.collection_temp, self.indexes_temp, function(err) {
                  // We're done with the copying. Now we can use new compacted collection seamlessly
                  var old_collection = self.collection;
                  self.collection = self.collection_temp;
                  self.index = self.index_temp;
                  self.indexes = self.indexes_temp;
                  delete self.index_temp;
                  delete self.indexes_temp;
                  delete self.collection_temp;
                  delete self.collection_temp_callback;
                  delete self.compact_in_progress;
                  
                  old_collection.end(function(err) {
                    if (err) {
                      callback(err);
                    } else {
                      old_collection.destroy(function(err) {
                        if (err) {
                          callback(err);
                        } else {
                          callback(null);
                        }
                      });
                    }
                  })

                });
              }
            });
          }
        })
      };
      
      if (exists) {
        fs.unlink(temp_collection_path, function(err) {
          if (err) {
            callback(err);
          } else {
            done_removing();
          }
        }); 
      } else {
        done_removing();
      }
    });
  }
};