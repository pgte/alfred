var carrier = require('carrier'),
    assert  = require('assert'),
    util             = require('util'),
    EventEmitter     = require('events').EventEmitter,
    options_merger = require('./util/options_merger');
    
var File          = require('./files/file'),
    BufferedFile  = require('./files/buffered_file');

var default_options = {
  buffered: true,
};

var Collection = function(file_path, options, callback) {
  var self = this;
  
  this.options = options_merger.merge(default_options, options);
  
  var file_class = File;
  if (this.options.buffered) {
    file_class = BufferedFile;
  }
  file_class.open(file_path, options, function(err, file) {
    if (err) {
      callback(err);
    } else {
      file.on('error', function(err) {
        self.emit('error', err);
      });
      file.on('before_flush', function() {
        self.emit('before_flush');
      });
      file.on('after_flush', function() {
        self.emit('after_flush');
      });
      self.file = file;
      callback(null, self);
    }
  });
};

util.inherits(Collection, EventEmitter);

/* collection.open(file_path [,options ], callback)  */
module.exports.open = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  
  return new Collection(file_path, options, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      callback(null, coll);
    }
  });
};

Collection.prototype.encodeObject = function(object) {
  return "\n" + JSON.stringify(object) + "\n";
};

Collection.prototype.write = function(record, callback) {
  if (record === null) {
    callback(new Error("collection.write called with null record value"));
    return;
  }
  this.file.write(this.encodeObject(record), callback);
};

Collection.prototype.writeAtPos = function(record, pos, callback) {
  if (record === null) {
    callback(new Error("collection.writeAtPos called with null record value"));
    return;
  }
  this.file.rawWrite(this.encodeObject(record), pos, callback);
};

Collection.prototype.end = function(callback) {
  this.file.end(callback);
};

Collection.prototype.endSync = function() {
  return this.file.endSync();
};

Collection.prototype.clear = function(callback) {
  this.file.clear(callback);
};

Collection.prototype.destroy = function(callback) {
  this.file.destroy(callback);
};
Collection.prototype.rename = function(new_name, callback) {
  this.file.rename(new_name, callback);
};

Collection.prototype.fetch = function(pos, length, callback) {
  var self = this;
  if (!length) {
    callback(new Error("invalid length"));
  }
  
  this.file.fetch(pos, length, function(err, record_string) {
    if (err) {
      callback(err);
    } else {
      var record;
      try {
        record = JSON.parse(record_string);
      } catch (excp) {
        callback(new Error('Error parsing "' + record_string + '" at pos ' + pos + ' from file ' + self.file.file_path));
        return;
      }
      callback(null, record);            
    }
  });
  
};

Collection.prototype.read = function(callback, null_on_end) {
  var self = this;

  this.file.writtenSize(function(err, size) {
    if (err) { callback(err); return; }
    if (size <= 0) {
      if (null_on_end) {
        callback(null, null);
      }
    } else {
      var file_pos = 0;
      var stop_at = size - 1;
      var record;
      var line_count = 0;
      (function keep_reading() {
        self.file.readOne(file_pos, function(err, str, bytesRead) {
          if (err) { callback(err); return; }
    
          if (str) {
            try {
              record = JSON.parse(str);
            } catch (excp) {
              process.nextTick(function() {
                callback(new Error("Error parsing line " + line_count + " of file " + self.file.file_path + " \"" + str + "\": " + excp.message));
              });
              return;
            }
            callback(null, record, file_pos, Buffer.byteLength(str) + 54);
          }
          file_pos += bytesRead;
          line_count += 2;

          if (file_pos < stop_at) {
            process.nextTick(function() {
              keep_reading(file_pos);
            });
          } else {
            if (null_on_end) {
              callback(null, null);
            }
          }
        });
      })();
    }
  });
};

Collection.prototype.filter = function(filter_function, callback) {
  this.read(function(error, record) {
    if (error) {
      callback(error);
    } else {
      if (record === null) { // reached the end
        callback(null, null);
      } else {
        if (filter_function(record)) {
          callback(null, record);
        }
      }
    }
  }, true);
};

Collection.prototype.position = function(pos) {
  this.file.position(pos);
};