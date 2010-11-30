var path    = require('path'),
    fs      = require('fs'),
    sys  = require('sys') || require('util'),
    events = require('events');

var MAX_WRITE_TRIES = 10;

var File = module.exports.klass = function(file_path, callback) {
  file_path = path.normalize(file_path);
  this.file_path = file_path;
  var self = this;
  this.written_size(function(err, size) {
    if (err) {
      callback(err);
    } else {
      self.write_pos = size;
      self.openFile(function(err) {
        if (err) {
          callback(err);
        } else {
          callback(null, self);
        }
      });
    }
  });
};

sys.inherits(File, events.EventEmitter);

module.exports.open = function(file_path, callback) {
  return new File(file_path, callback);
};

File.prototype.openFile = function(callback) {
  var self = this;
  fs.open(self.file_path, 'a+', 0600, function(err, fd) {
    if (err) {
      callback(err);
    } else {
      self.fd = fd;
      callback(null);
    }
  });
};


File.prototype.write = function(string, callback) {
  var pos = this.write_pos;
  this.write_pos += Buffer.byteLength(string);
  this.raw_write(string, pos, callback);
};

File.prototype.raw_write = function(string, pos, callback) {
  var buffer = new Buffer(string),
      length = Buffer.byteLength(string),
      self = this,
      tries = 0;
      
  (function tryWrite () {
    tries ++;
    fs.write(self.fd, buffer, 0, length, pos, function(err, written) {
      if (err) {
        callback(err);
      } else {
        if (written == length) {
          callback(null, pos, length);
        } else {
          tryWrite();
        }
      }
    });
  })();
};

File.prototype.raw_write_sync = function(string, pos) {
  var buffer = new Buffer(string),
      length = Buffer.byteLength(string),
      written = 0;
    
  while(written < length) {
    written = fs.writeSync(this.fd, buffer, 0, length, pos);
  } 
  return written;
};

File.prototype.end = function(callback) {
  fs.close(this.fd, callback);
};

File.prototype.destroy = function(callback) {
  var self = this;
  path.exists(this.file_path, function(exists) {
    if (exists) {
      fs.unlink(self.file_path, function(err) {
        if (err) {
          callback(err);
        } else {
          callback(null);
        }
      });
    }
  });
};

File.prototype.rename = function(new_name, callback) {
  var self = this;
  fs.rename(this.file_path, new_name, function(err) {
    if (err) {
      callback(err);
    } else {
      self.file_path = new_name;
      callback(null);
    }
  });
};

File.prototype.clear = function(callback) {
  var self = this;
  
  self.end(function(err) {
    if (err) {
      callback(err);
    } else {
      path.exists(self.file_path, function(exists) {
        if (exists) {
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

File.prototype.fetch = function(pos, length, callback) {
  var buf = new Buffer(length),
      bytesRead = 0,
      self = this;
  (function tryRead() {
    fs.read(self.fd, buf, bytesRead, length - bytesRead, pos + bytesRead, function(err, bytesReadNow) {
      if (err) {
        callback(err);
      } else {
        bytesRead += bytesReadNow;
        if (bytesRead == length) {
          callback(null, buf.toString('utf-8', 0, bytesRead));
        } else {
          tryRead();
        }
      }
    });
  })();
};

File.prototype.try_read = function(pos, length, callback) {
  var buf = new Buffer(length);
  fs.read(this.fd, buf, 0, length, pos, function(err, bytesRead) {
    if (err) {
      callback(err);
    } else {
      callback(null, buf.toString('utf-8', 0, bytesRead));
    }
  });
};

File.prototype.size = function(callback) {
  callback(null, this.write_pos || 0);
};

File.prototype.written_size = function(callback) {
  var self = this;
  path.exists(this.file_path, function(exists) {
    if (exists) {
      fs.stat(self.file_path, function(err, stat) {
        if (err) {
          callback(err);
        } else {
          callback(null, stat.size);
        }
      });
    } else {
      callback(null, 0);
    }
  });
};

File.prototype.position = function(pos) {
  this.write_pos = pos;
};