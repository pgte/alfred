var util    = require('util'),
    assert = require('assert'),
    File   = require('./file').klass,
    options_merger = require('../util/options_merger');

var default_options = {
  flush_interval_ms: 1000
};

    
var BufferedFile = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  
  this.options = options_merger.merge(default_options, options);
  
  this.flush_callback_queue = [];
  
  File.call(this, file_path, this.options, function(err) {
    if (err) {
      callback(err);
    } else {
      
      callback(null, self);
    }
  });
};

util.inherits(BufferedFile, File);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  return new BufferedFile(file_path, options, callback);
};

BufferedFile.prototype.openFile = function(callback) {
  var self = this;
  
  File.prototype.openFile.call(this, function(err) {
    if (err) {
      callback(err);
    } else {
      self.buffered_file_ended = false;
      self.queue = [];
      self.segment_map = {};
      self.queue_starts_at_pos = self.write_pos;

      var scheduleFlush = function() {
        self.flush_timeout = setTimeout(function() {
          self.flush(function(err) {
            if (err) {
              self.emit('error', err);
            } else {
              if (!self.buffered_file_ended) {
                scheduleFlush();
              }
            }
          });
        }, self.options.flush_interval_ms);
      };
      scheduleFlush();
      
      self.buffered_file_on_exit = function() {
        self.flushSync();
      };
      process.on('exit', self.buffered_file_on_exit);
      
      callback(null);
    }
  });
};

BufferedFile.prototype.write = function(string, callback) {
  var length = Buffer.byteLength(string, 'utf8') + 54;
  var pos = this.write_pos;
  var segment = {
    s: string,
    p: pos,
    l: length
  };
  this.queue.push(segment);
  this.segment_map[pos] = segment;
  this.write_pos += length;
  callback(null, pos, length);
};

BufferedFile.prototype.flush = function(callback) {
  var self = this;
  
  if (this.queue.length === 0) {
    callback(null);
    return;
  }
  
  self.emit('before_flush');
  
  self.flush_callback_queue.push(callback);

  if (self.flushing) {
    return;
  }
  self.flushing = true;
  
  var all_callback = function(err) {
    while (self.flush_callback_queue.length > 0) {
      var cb = self.flush_callback_queue.splice(0, 1)[0];
      cb(err);
    }
  }
  
  var flushSegment = function() {
    var segment = self.queue[0];
    File.prototype.rawWrite.call(self, segment.s, segment.p, function(err, pos, length) {
      if (err) {
        all_callback(err);
      } else {
        if (pos != segment.p) {
          all_callback();
        } else {
          if (length != segment.l) {
            all_callback(new Error('File.rawWrite returned a different length from what was requested. Returned ' + length + ', and had requested ' + segment.l));
          } else {
            delete self.segment_map[pos];
            self.queue_starts_at_pos += length;
            self.queue.splice(self.queue.indexOf(segment), 1);
            if (self.queue.length === 0) {
              delete self.flushing;
              all_callback(null);
              process.nextTick(function() {
                self.emit('after_flush');
              });
            } else {
              flushSegment();
            }
          }
        }
      }
    });
    
  };
  flushSegment();
};

BufferedFile.prototype.flushSync = function() {
  while (this.queue.length > 0) {
    var segment = this.queue.splice(0, 1)[0];
    var written = File.prototype.rawWrite_sync.call(this, segment.s, segment.p);
  }
};

BufferedFile.prototype.fetch = function(pos, length, callback) {
  if (pos >= this.queue_starts_at_pos) {
    var segment = this.segment_map[pos];
    if (segment) {
      callback(null, segment.s);
    } else {
      callback(new Error("position " + pos + " not found in queue for file " + this.file_path));
    }
  } else {
    File.prototype.fetch.call(this, pos, length, callback);
  }
};

BufferedFile.prototype.readOne = function(pos, callback) {
  var found = false;
  var queue_pos = 0;
  if (this.queue.length > 0 && pos >= this.queue_starts_at_pos) {
    while(queue_pos < this.queue.length) {
      current_segment = this.queue[queue_pos];
      if (current_segment.p == pos) {
        found = true;
        callback(null, current_segment.s, current_segment.l);
      }
      queue_pos ++;
    }
    if (!found) {
      callback(null, null, null);
    }
  } else {
    File.prototype.readOne.call(this, pos, callback);
  }
};

BufferedFile.prototype.end = function(callback) {
  var self = this;
  if (this.flush_timeout) {
    clearTimeout(this.flush_timeout);
  }
  this.buffered_file_ended = true;
  this.flush(function(err) {
    if (err) {
      callback(err);
    } else {
      File.prototype.end.call(self, function(err) {
        if (err) {
          callback(err);
        } else {
          process.removeListener('exit', self.buffered_file_on_exit);
          callback(null);
        }
      });
    }
  });
  
};

BufferedFile.prototype.endSync = function(callback) {
  var self = this;
  if (this.flush_timeout) {
    clearTimeout(this.flush_timeout);
  }
  this.buffered_file_ended = true;
  this.flushSync();
  process.removeListener('exit', self.buffered_file_on_exit);
  return File.prototype.endSync.call(self);
};

BufferedFile.prototype.clear = function(callback) {
  var self = this;
  this.queue = [];
  this.segment_map = {};
  File.prototype.clear.call(this, function(err) {
    if (err) {callback(err); return;}
    self.queue_starts_at_pos = 0;
    callback(null);
  });
};

BufferedFile.prototype.writtenSize = function(callback) {
  if (this.write_pos) {
    callback(null, this.write_pos);
  } else {
    File.prototype.writtenSize.call(this, callback);
  }
  
};

File.prototype.position = function(pos) { 
  this.queue_starts_at_pos = this.write_pos = pos;
};