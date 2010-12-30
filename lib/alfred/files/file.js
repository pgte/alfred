var path          = require('path'),
    fs            = require('fs'),
    util          = require('util'),
    events        = require('events'),
    child_process = require('child_process');

var MAX_WRITE_TRIES = 10;

var File = module.exports.klass = function(file_path, callback) {
  file_path = path.normalize(file_path);
  this.file_path = file_path;
  this.queue_size = 0;
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

util.inherits(File, events.EventEmitter);

module.exports.open = function(file_path, callback) {
  return new File(file_path, callback);
};

File.prototype.openFile = function(callback) {
  var self = this;
  this._acquireLock(function(err) {
    if (err) { callback(err); return; }
    fs.open(self.file_path, 'a+', 0600, function(err, fd) {
      if (err) {
        callback(err);
      } else {
        if (self._rawWrite) {
          self.rawWrite = self._rawWrite;
        }
        if (self._fetch) {
          self.fetch = self._fetch;
        }
        self.fd = fd;
        callback(null);
      }
    });
  });
};

File.prototype.write = function(string, callback) {
  var pos = this.write_pos;
  this.write_pos += Buffer.byteLength(string);
  this.rawWrite(string, pos, callback);
};

File.prototype.rawWrite = function(string, pos, callback) {
  var buffer = new Buffer(string),
      length = Buffer.byteLength(string),
      self = this;
  
  this.queue_size ++;
      
  (function tryWrite () {
    fs.write(self.fd, buffer, 0, length, pos, function(err, written) {
      if (err) {
        self.queue_size --;
        callback(err);
        self._needsEnd();
      } else {
        if (written == length) {
          self.queue_size --;
          callback(null, pos, length);
          self._needsEnd();
        } else {
          tryWrite();
        }
      }
    });
  })();
};

File.prototype.rawWrite_sync = function(string, pos) {
  var buffer = new Buffer(string),
      length = Buffer.byteLength(string),
      written = 0;
    
  while(written < length) {
    written = fs.writeSync(this.fd, buffer, 0, length, pos);
  } 
  return written;
};

File.prototype.rename = function(new_name, callback) {
  var self = this;
  fs.rename(this.file_path, new_name, function(err) {
    if (err) {
      callback(err);
    } else {
      var old_lock_path = self._lockFilePath();
      self.file_path = new_name;
      path.exists(old_lock_path, function(exists) {
        if (exists) {
          fs.rename(old_lock_path, self._lockFilePath(), callback);
        } else {
          callback(null);
        }
      });
      
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
      
  this.queue_size ++;
  
  (function tryRead() {
    fs.read(self.fd, buf, bytesRead, length - bytesRead, pos + bytesRead, function(err, bytesReadNow) {
      if (err) {
        this.queue_size --;
        callback(err);
        self._needsEnd();
      } else {
        bytesRead += bytesReadNow;
        if (bytesRead == length) {
          self.queue_size --;
          callback(null, buf.toString('utf-8', 0, bytesRead));
          self._needsEnd();
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
      var retBuf = new Buffer(bytesRead);
      buf.copy(retBuf, 0, 0, bytesRead);
      callback(null, retBuf, bytesRead);
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

/* End */


File.prototype._needsEnd = function() {
  if (this.ending && this.queue_size === 0) {
    this._forceEnd(this.ending);
  }
};

File.prototype.end = function(callback) {
  if (this.ending) {
    callback(new Error("File is already ending"));
  }
  if (this.queue_size === 0) {
    this._forceEnd(callback);
  } else {
    this.ending = callback;
  }
};

File.prototype.endSync = function(callback) {
  if (this.ending) {
    callback(new Error("File is already ending"));
  }
  return fs.closeSync(this.fd);
};

File.prototype._forceEnd = function(callback) {
  var self = this;
  
  fs.close(this.fd, function(err) {
    if (err) {
      callback(err);
    } else {
      delete self.ending;
      var message = 'File is closed';
      self._fetch = self.fetch;
      self.fetch = function(pos, length, callback) {
        callback(new Error(message));
      };
      self._rawWrite = self.rawWrite;
      self.rawWrite = function(string, pos, callback) {
        callback(new Error(message));
      };
      self._releaseLock(function(err) {
        if (err) {callback(err); return; }
        callback(null);
      });
    }
  });
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

/* Locking */

File.prototype._lockFilePath = function(callback) {
  return this.file_path + '.lock';
};

File.prototype._acquireLock = function(callback) {
  var self = this,
      lock_file_path = this._lockFilePath();
  
  path.exists(lock_file_path, function(exists) {
    if (exists) {
      fs.readFile(lock_file_path, function(err, locked_by_pid) {
        if (err) { callback(err); return; }
        var command = 'ps ' + locked_by_pid + ' | grep ' + locked_by_pid;
        child_process.exec(command, {timeout: 1000}, function(err, stdout, stderr) {
          if (err) {
            // the process is no longer running. we may claim the file
            fs.unlink(lock_file_path, function(err) {
              if (err) { callback(err); return; }
              return self._acquireLock(callback);
            });
          } else {
            callback(new Error(self.file_path + ' is locked by process with PID ' + locked_by_pid));
          }
        });
        
      });
    } else {
      fs.writeFile(lock_file_path, process.pid.toString(), 'utf8', function(err) {
        if (err) { callback(err); return; }
        callback(null);
      });
    }
  });
};

File.prototype._releaseLock = function(callback) {
  var self = this,
      lock_file_path = this._lockFilePath();
  
  if (this.releasing_lock) {
    callback(new Error('already releasing lock'));
    return;
  }
  this.releasing_lock = true;
  
  path.exists(lock_file_path, function(exists) {
    if (!exists) {
      callback(new Error('Lock file ' + lock_file_path + ' does not exist'));
    } else {
      fs.readFile(lock_file_path, function(err, locked_by_pid) {
        if (err) { callback(err); return; }
        if (locked_by_pid != process.pid.toString()) {
          callback(new Error('Cannot unlock. Lock file ' + lock_file_path + ' locked by another process with PID ' + locked_by_pid));
          return;
        }
        fs.unlink(lock_file_path, function(err) {
          delete self.releasing_lock;
          callback(err);
        });
      });
    }
  });
  
};