var path                = require('path'),
    fs                  = require('fs'),
    sys                 = require('sys') || require('util'),
    coordinator         = require('./coordinator');
    collection          = require('./collection');

var CompactCoordinator = function(key_map) {
  this.key_map = key_map;
};

sys.inherits(CompactCoordinator, coordinator.klass);

module.exports.create = function(key_map) {
  return new CompactCoordinator(key_map);
};

CompactCoordinator.prototype.copyIntoCollectionAndIndexes = function(collection, indexes, callback) {
  
  var self = this;
  
  this.key_map.index_temp = this.key_map.index.clone();
  var index_temp_temp = this.key_map.index_temp.clone();
  
  var count = this.key_map.index_temp.count();
  var copied_count = 0;
  var current_pos = 0;
  index_temp_temp.each(function(err, key, rec) {
    self.key_map.index_temp.atomic(key, null, function(err, secret, other_rec, next) {
      if (err) {
        callback(err);
        return
      }
      self.key_map.get_at_pos(rec.o, rec.l, function(err, key, value) {
        if (err) {
          callback(err);
          return;
        }
        var this_pos = current_pos;
        current_pos += rec.l;
        self.key_map.put_at_pos(key, value, this_pos, function(err, position, length) {
          if (err) {
            callback(err);
          } else {
            self.key_map.index_temp.put(key, position, length, function(err) {
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

CompactCoordinator.prototype.start = function(callback) {
  var self = this;
  
  var temp_collection_path = this.key_map.file_path + '.temp';
  path.exists(temp_collection_path, function(exists) {
    
    var done_removing = function() {
      collection.open(temp_collection_path, function(err, coll) {
        if (err) {
          callback(err);
        } else {
          self.key_map.collection_temp_callback = callback;
          self.key_map.collection_temp = coll;
          self.key_map.size(function(err, size) {
            if (err) {
              callback(err);
            } else {
              
              self.key_map.indexes_temp = {};
              var index, cloned_index;
              for(var index_index in self.key_map.indexes) {
                if (self.key_map.indexes.hasOwnProperty(index_index)) {
                  index = self.key_map.indexes[index_index];
                  cloned_index = new functional_index.open(index.transformFunction());
                  self.key_map.indexes_temp[index_index] = cloned_index;
                }
              }
              
              self.key_map.collection_temp.position(size);
              self.copyIntoCollectionAndIndexes(self.key_map.collection_temp, self.key_map.indexes_temp, function(err) {
                // We're done with the copying. Now we can use new compacted collection seamlessly
                var old_collection = self.key_map.collection;
                self.key_map.collection = self.key_map.collection_temp;
                self.key_map.index = self.key_map.index_temp;
                self.key_map.indexes = self.key_map.indexes_temp;
                delete self.key_map.index_temp;
                delete self.key_map.indexes_temp;
                delete self.key_map.collection_temp;
                delete self.key_map.collection_temp_callback;
                delete self.key_map.compact_in_progress;
                
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
};


CompactCoordinator.prototype.notify_put = function() {
  var self = this;
  if (self.key_map.collection_temp) { // If we are in the middle of a compactation
    KeyMap.prototype.put.call(self.key_map, key, value, function(err, position, length) {
      if (err) {
        self.key_map.collection_temp_callback(err);
      } else {
        self.key_map.index_temp.put(key, position, length, function(err) {
          if (err) {
            self.key_map.collection_temp_callback(err);
          }
        });
      }
    }, self.key_map.collection_temp);
  }
  
};