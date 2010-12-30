var carrier = require('carrier'),
    assert  = require('assert'),
    util             = require('util'),
    EventEmitter     = require('events').EventEmitter,
    options_merger = require('./util/options_merger');
    
var File          = require('./files/file'),
    BufferedFile  = require('./files/buffered_file'),
    StringDecoder = require('string_decoder').StringDecoder;

var default_options = {
  buffered: true,
  read_buffer_size:  40 * 1024
  //read_buffer_size:  1024
};

var Collection = function(file_path, options, callback) {
  var self = this;
  
  this.options = options_merger.merge(default_options, options);
  
  var file_class = File;
  if (this.options.buffered) {
    file_class = BufferedFile;
  }
  file_class.open(file_path, function(err, file) {
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

Collection.prototype.write = function(record, callback) {
  if (record === null) {
    callback(new Error("collection.write called with null record value"));
    return;
  }
  this.file.write("\n" + JSON.stringify(record) + "\n", callback);
};

Collection.prototype.writeAtPos = function(record, pos, callback) {
  if (record === null) {
    callback(new Error("collection.writeAtPos called with null record value"));
    return;
  }
  this.file.rawWrite("\n" + JSON.stringify(record) + "\n", pos, callback);
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
    
    if (err) {
      callback(err);
      return;
    }
    
    var file_pos = 0;
    var line_pos = 0;
    var line = '';
    var line_count = 0;
    var string_decoder = new StringDecoder('utf8');
    
    if (size <= 0) {
      if (null_on_end) {
        callback(null, null);
      }
    } else {
      var stop_at = size - 1;
      
      (function keep_reading(file_pos) {

        self.file.tryRead(file_pos, self.options.read_buffer_size, function(err, ret_buffer, bytesRead) {
          if (err) {
            callback(err);
            return;
          }
          if (!ret_buffer) {
            return;
          }
          var content = string_decoder.write(ret_buffer);
          var lines = content.split("\n");
          if(content.charAt(content.length - 1) == "\n") {
            lines.pop(1);
          }
          lines.forEach(function(one_line, index) {
            line += one_line;
            var emit = true;
            var line_length = Buffer.byteLength(one_line);
            if (index == (lines.length - 1)) {
              // last line of chunk
              if (file_pos + line_length < stop_at && content.charAt(content.length - 1) != "\n") {
                emit = false;
              } else {
                line_length ++;
              }
            } else {
              line_length ++;
            }
            if (emit && line.length > 0) {
              var record;
              try {
                line_count += 2;
                record = JSON.parse(line);
              } catch(excp) {
                process.nextTick(function() {
                  callback(new Error("Error parsing line " + line_count + " of file " + self.file.file_path + " \"" + line + "\": " + excp.message));
                });
                return;
              }
              var completed_line_length  = Buffer.byteLength(line) + 2;
              var old_line_pos = line_pos;
              line = '';
              line_pos += completed_line_length;
              callback(null, record, old_line_pos, completed_line_length);
            }
          });
          file_pos += bytesRead;
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
      })(file_pos);
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