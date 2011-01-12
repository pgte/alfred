var path                = require('path'),
    fs                  = require('fs'),
    util                = require('util'),
    assert              = require('assert'),
    coordinator         = require('./coordinator');
    collection          = require('./../collection'),
    KeyMap              = require('./../key_map').klass,
    OrderedIndex        = require('./../indexes/ordered_index'),
    UnorderedIndex        = require('./../indexes/unordered_index');

var CompactCoordinator = function(key_map, options) {
  this.key_map = key_map;
  this.options = options;
};

util.inherits(CompactCoordinator, coordinator.klass);

module.exports.create = function(key_map, options) {
  return new CompactCoordinator(key_map, options);
};

CompactCoordinator.prototype.copyIntoCollectionAndIndexes = function(collection, indexes, callback) {
  
  var self = this;
  
  this.index_temp = this.key_map.index.clone();
  var index_temp_temp = this.index_temp.clone();
  
  var count = this.index_temp.count();
  var copied_count = 0;
  var current_pos = 0;
  
  assert.equal(index_temp_temp.count(), count);
  
  index_temp_temp.each(function(err, key, pos, length) {
    process.nextTick(function() {
      self.index_temp.atomic(key, null, function(err, secret, other_rec, next) {
        if (err) {
          callback(err);
          return
        }
        self.key_map.getAtPos(pos, length, function(err, key, value) {
          if (err) {
            callback(err);
            return;
          }
          var this_pos = current_pos;
          current_pos += length + 6;
          self.key_map.putAtPos(key, value, this_pos, function(err, position, length) {
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
  });
};

CompactCoordinator.prototype.start = function(callback) {
  var self = this;
  
  if (this.started) {
    callback(new Error("Already started coordinator"));
    return
  }
  this.started = true;
  this.callback = callback;
  
  var original_file_name = this.key_map.file_path;
  var temp_collection_path = original_file_name + '.temp';
  path.exists(temp_collection_path, function(exists) {
    
    var done_removing = function() {
      collection.open(temp_collection_path, self.options, function(err, coll) {
        if (err) {
          callback(err);
        } else {
          self.collection_temp = coll;
          self.key_map.size(function(err, size) {
            if (err) {
              callback(err);
            } else {
              
              self.key_map.indexes_temp = {};
              var index, cloned_index;
              for(var index_index in self.key_map.indexes) {
                if (self.key_map.indexes.hasOwnProperty(index_index)) {
                  index = self.key_map.indexes[index_index];
                  cloned_index = new (index.ordered ? OrderedIndex : UnorderedIndex).open(index.transformFunction);
                  self.key_map.indexes_temp[index_index] = cloned_index;
                }
              }
              
              self.collection_temp.position(size);
              self.copyIntoCollectionAndIndexes(self.collection_temp, self.key_map.indexes_temp, function(err) {
                if (err) {
                  callback(err);
                  return;
                }
                // We're done with the copying. Now we can use new compacted collection seamlessly
                var old_collection = self.key_map.collection;
                
                var new_collection = self.collection_temp;
                // attach event handlers to new collection
                new_collection.on('error', function(err) {
                  new_collection.emit('error', err);
                });

                new_collection.on('before_flush', function() {
                  new_collection.emit('before_flush');
                });

                new_collection.on('after_flush', function() {
                  new_collection.emit('after_flush');
                });
                
                self.key_map.collection = self.collection_temp;
                self.key_map.index = self.index_temp;
                self.key_map.indexes = self.key_map.indexes_temp;
                delete self.index_temp;
                delete self.key_map.indexes_temp;
                delete self.collection_temp;
                
                old_collection.end(function(err) {
                  if (err) {
                    callback(err);
                  } else {
                    old_collection.destroy(function(err) {
                      self.key_map.rename(original_file_name, function(err) {
                        if (err) {
                          callback(err);
                        } else {
                          self.started = false;
                          process.nextTick(function() {
                            callback(null);
                          });
                        }
                      })
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
};


CompactCoordinator.prototype.notify_put = function(key, value, callback) {
  var self = this;
  if (self.collection_temp) { // If we are in the middle of a compactation
    KeyMap.prototype.put.call(self.key_map, key, value, function(err, position, length) {
      if (err) {
        self.callback(err);
      } else {
        self.index_temp.put(key, position, length, function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        });
      }
    }, self.collection_temp);
  }
  
};