var path          = require('path'),
    fs            = require('fs'),
    util          = require('util'),
    events        = require('events'),
    child_process = require('child_process'),
    options_merger = require('../util/options_merger'),
    crypto        = require('crypto'),
    StringDecoder = require('string_decoder').StringDecoder;

var default_options = {
  buffer_length: 16 * 1024
};

var File = module.exports.klass = function(file_path, options, callback) {
  file_path = path.normalize(file_path);  
  this.file_path = file_path;
  this.options = options_merger.merge(default_options, options);
  
  this.queue_size = 0;
  var self = this;
  this.writtenSize(function(err, size) {
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

module.exports.open = function(file_path, options, callback) {
  return new File(file_path, options, callback);
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
  this.write_pos += Buffer.byteLength(string) + 52;
  this.rawWrite(string, pos, callback);
};

/* Header */

/* 40 bytes header */
var calculateHeader = function(string, pos) {
  ret = "000000000000" + Buffer.byteLength(string);
  return ret.substring(ret.length - 12);
};

/* Trailer */

/* 12 bytes trailer */
var calculateTrailer = function(string, pos) {
  var hash = crypto.createHash('sha1');
  hash.update(string);
  // pos is 12 bytes
  // MD5 / SHA1 base64 hash max is 28 bytes
  var hashString = '############################' + hash.digest('base64');
  hashString = hashString.substring(hashString.length - 28);
  var ret = "000000000000" + pos.toString() + hashString;
  //return a 40 byte trailer
  return ret.substring(ret.length - 40);
};

var validateTrailer = function(trailer, string, pos) {
  return calculateTrailer(string, pos) == trailer;
};

var getAndValidateRecordFromBuffer = function(buf, pos, self) {
  var header = buf.toString('utf8', 0, 12);
  var length = parseInt(header, 10);
  var record = buf.toString('utf8', 12, 12 + length);
  var trailer = buf.toString('utf8', 12 + length, 52 + length);
  if (!validateTrailer(trailer, record, pos)) {
    throw new Error('Error validating trailer on pos ' + pos + ' of file ' + self.file_path + '. Should be ' + calculateTrailer(record, pos) + ' and is ' + trailer);
  }
  return record;
};

/* Write */
File.prototype.rawWrite = function(string, pos, callback) {
  var buffer = new Buffer(calculateHeader(string, pos) + string + calculateTrailer(string, pos)),
      length = buffer.length,
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
  var buffer = new Buffer(calculateHeader(string, pos) +string + calculateTrailer(string, pos)),
      length = buffer.length,
      written = 0;
    
  while(written < length) {
    written = fs.writeSync(this.fd, buffer, 0, length, pos);
  } 
  return written;
};

/* Read */

File.prototype.fetch = function(pos, length, callback) {
  
  var buf = new Buffer(length),
      bytesRead = 0,
      self = this,
      record;
      
  this.queue_size ++;
  
  (function tryRead() {
    fs.read(self.fd, buf, bytesRead, length - bytesRead, pos + bytesRead, function(err, bytesReadNow) {
      if (err) {
        self.queue_size --;
        callback(err);
        self._needsEnd();
      } else {
        bytesRead += bytesReadNow;
        if (bytesRead >= length) {
          self.queue_size --;
          try {
            record = getAndValidateRecordFromBuffer(buf.slice(0, length), pos, self);
          } catch (except) {
            self._needsEnd();
            callback(except);
            return;
          }
          callback(null, record);
          self._needsEnd();
        } else {
          process.nextTick(function() {
            tryRead();
          });
        }
      }
    });
  })();
};

File.prototype.readOne = function(position, callback) {
  var buf = new Buffer(this.options.buffer_length),
      read_string = '';
      bytesRead = 0,
      self = this,
      length = this.options.buffer_length,
      processed_header = false,
      string_decoder = new StringDecoder('utf8'),
      record = undefined;
      
  this.queue_size ++;
  
  (function tryRead() {
    fs.read(self.fd, buf, 0, buf.length, position + bytesRead, function(err, bytesReadNow) {
      if (err) {
        self.queue_size --;
        callback(err);
        self._needsEnd();
      } else {
        bytesRead += bytesReadNow;
        
        read_string += string_decoder.write(buf.slice(0, bytesReadNow));
        
        // if we haven't been able to read the header fully, try again
        if (bytesRead < 12) {
          tryRead();
          return;
        }
        if (!processed_header) {
          // now length is the record length + 40 * 2 (header is 12 bytes and trailer is 40 bytes)
          length = parseInt(read_string.substring(0, 12), 10) + 52;
          processed_header = true;
        }
        if (bytesRead >= length) {
          self.queue_size --;
          record = undefined;
          try {
            record = getAndValidateRecordFromBuffer(new Buffer(read_string).slice(0, length), position, self);
          } catch (except) {
            this.queue_size --;
            callback(except);
            return;
          }
          callback(null, record, length);
          this.queue_size --;
          self._needsEnd();
        } else {
          tryRead();
          return;
        }
      }
    });
  })();
};

File.prototype.size = function(callback) {
  callback(null, this.write_pos || 0);
};

File.prototype.writtenSize = function(callback) {
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

/* Other */

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
