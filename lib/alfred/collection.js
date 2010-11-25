var carrier = require('carrier'),
    assert  = require('assert'),
    options_merger = require('./options_merger');
    
var File         = require('./file');
    BufferedFile = require('./buffered_file');

var READ_BUFFER_SIZE = 40 * 1024;

var default_options = {
  buffered: true
};

var Collection = function(file_path, callback, options) {
  var self = this;
  options = options_merger.merge(default_options, options);
  
  var file_class = File;
  if (options.buffered) {
    file_class = BufferedFile;
  }
  file_class.open(file_path, function(err, file) {
    if (err) {
      callback(err);
    } else {
      self.file = file;
      callback(null, self);
    }
  });
};

module.exports.open = function(file_path, callback, options) {
  var self = this;
  new Collection(file_path, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      callback(null, coll);
    }
  }, options);
}

Collection.prototype.write = function(record, callback) {
  if (record == null) {
    callback(new Error("collection.write called with null record value"));
    return;
  }
  this.file.write(JSON.stringify(record) + "\n", callback);
};

Collection.prototype.end = function(callback) {
  this.file.end(callback);
};

Collection.prototype.clear = function(callback) {
  this.file.clear(callback);
};

Collection.prototype.fetch = function(pos, length, callback) {
  if (!length) {
    callback(new Error("invalid length"));
  }
  
  this.file.fetch(pos, length, function(err, record_string) {
    var record;
    try {
      record = JSON.parse(record_string);
    } catch (excp) {
      callback(new Error("Error parsing \"" + record_string + "\""));
      return;
    }
    callback(null, record);            
  });
  
};

Collection.prototype.read = function(callback, null_on_end) {
  var self = this;
  this.file.written_size(function(err, size) {
    
    if (err) {
      callback(err);
      return;
    }
    
    var file_pos = 0;
    var line_pos = 0;
    var line = '';
    
    if (size <= 0) {
      if (null_on_end) {
        callback(null, null);
      }
    } else {
      var stop_at = size - 1;
      
      (function keep_reading() {
        self.file.try_read(file_pos, READ_BUFFER_SIZE, function(err, content) {
          if (err) {
            callback(err);
          } else {
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
                  record = JSON.parse(line);
                } catch(excp) {
                  throw new Error("Error parsing \"" + line + "\"")
                }
                var completed_line_length  = Buffer.byteLength(line) + 1;
                callback(null, record, line_pos, completed_line_length);
                line_pos += completed_line_length;
                line = '';
              }
              file_pos += line_length;
            });
            if (file_pos < stop_at) {
              keep_reading();
            } else {
              if (null_on_end) {
                callback(null, null);
              }
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