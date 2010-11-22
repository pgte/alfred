var fs      = require('fs'),
    carrier = require('carrier'),
    assert  = require('assert'),
    path    = require('path');

var READ_BUFFER_SIZE = 40 * 1024;

var Collection = function(file_path, callback) {
  var self = this;
  file_path = path.normalize(file_path);
  fs.stat(file_path, function(err, stats) {
    if (err) {
      callback(err);
    }
    self.file_path = file_path;
    self.write_pos = stats.size;
    callback();
  });
};

module.exports.open = function(file_path, callback) {
  var coll = new Collection(file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      coll.openFile(function(err) {
        callback(err, coll);
      });
    }
  });
}

Collection.prototype.openFile = function(callback) {
  var self = this;
  fs.open(self.file_path, 'a', 0666, function(err, fd) {
    if (err) {
      callback(err);
    } else {
      self.fd = fd;
      callback(null);
    }
  });
};

Collection.prototype.write = function(record, callback) {
  var str = JSON.stringify(record) + "\n";
  var length = Buffer.byteLength(str);
  var buffer = new Buffer(str);
  fs.write(this.fd, buffer, 0, length, this.write_pos, function(err, written) {
    if (err) {
      callback(err);
    } else {
      assert.equal(length, written, 'Written length is not the same as predicted length');
      callback(null);
    }
  });
  this.write_pos += length;
};

Collection.prototype.end = function(callback) {
  fs.close(this.fd, callback);
};

Collection.prototype.clear = function(callback) {
  var self = this;
  self.end(function(err) {
    if (err) {
      callback(err);
    } else {
      fs.unlink(self.file_path, function(err) {
        if (err) {
          callback(err);
        } else {
          self.openFile(function(err) {
            self.write_pos = 0;
            callback(err);
          });
        }
      });
    }
  });
};

Collection.prototype.read = function(record_handler) {
  var rs = fs.createReadStream(this.file_path);
  rs.on('error', function(error) {
    record_handler(error);
  });
  rs.on('end', function() {
    try {
      
    } catch (excp) {
      rs.destroy();
    }
    record_handler(null, null);
  });
  carrier.carry(rs, function(line) {
    var record = JSON.parse(line);
    record_handler(null, record);
  });
};

Collection.prototype.busy = function(callback) {
  var self = this;
  if (this.busy_working) {
    this.busy_queue.push(callback)
  } else {
    this.busy_working = true;
    this.busy_queue = [];
    callback(function do_the_next() {
      if (self.busy_queue.length > 0) {
        var next_callback = self.busy_queue.pop();
        next_callback(do_the_next);
      } else {
        self.busy_working = false;
      }
    });
  }
}

Collection.prototype.fetch = function(pos, length, callback) {
  var self = this;
  if (!length) {
    callback(new Error("invalid length"));
  }
  this.busy(function(next) {
    fs.open(self.file_path, 'r', null, function(err, fd) {
      if (err) {
        callback(err);
      } else {
        buf = new Buffer(length);
        fs.read(fd, buf, 0, length, pos, function(err, bytesRead) {
          assert.equal(bytesRead, length, "asked to read " + length + " bytes and got " + bytesRead);
          var record_string = buf.toString('utf-8', 0, bytesRead);
          var record;
          try {
            record = JSON.parse(record_string);
          } catch (excp) {
            throw new Error("Error parsing \"" + record_string + "\"");
          }
          callback(null, record);            
          fs.close(fd);
          next();
        });
      }
    });
  });
};

Collection.prototype.read_with_pos = function(callback) {
  var self = this;
  this.busy(function(next) {

    fs.open(self.file_path, 'r', null, function(err, fd) {
      if (err) {
        callback(err);
      } else {
        fs.stat(self.file_path, function(err, stat) {
          var buf = new Buffer(100);
          var file_pos = 0;
          var line_pos = 0;
          var line = '';

          var stop_at = stat.size - 1;

          (function keep_reading() {
            fs.read(fd, buf, 0, buf.length, file_pos, function(err, bytesRead) {
              if (err) {
                callback(err);
              } else {
                var content = buf.toString('utf-8', 0, bytesRead);
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
                  fs.close(fd);
                  next();
                }
              }
            });
          })();
        });
      }
    });    
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
  });
};