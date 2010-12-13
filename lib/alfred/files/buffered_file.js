var sys    = require('sys') || require('util'),
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
    callback = arguments[2];
    options = arguments[1];
  }
  
  this.options = options_merger.merge(default_options, options);
  
  File.call(this, file_path, function(err) {
    if (err) {
      callback(err);
    } else {
      
      callback(null, self);
    }
  });
};

sys.inherits(BufferedFile, File);

module.exports.open = function(file_path, callback) {
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    callback = arguments[2];
    options = arguments[1];
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
              if (self.listeners('event').length > 0) {
                self.emit('error', err);
              } else {
                throw err;
              }
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
  var length = Buffer.byteLength(string);
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

  var flushSegment = function() {
    var segment = self.queue.splice(0, 1)[0];
    File.prototype.raw_write.call(self, segment.s, segment.p, function(err, pos, length) {
      if (err) {
        callback(err);
      } else {
        if (pos != segment.p) {
          callback(new Error('File.raw_write returned a different pos from what was requested. Returned ' + pos + ', and had requested ' + segment.p));
        } else {
          if (length != segment.l) {
            callback(new Error('File.raw_write returned a different length from what was requested. Returned ' + length + ', and had requested ' + segment.l));
          } else {
            delete self.segment_map[pos];
            self.queue_starts_at_pos += length;
            if (self.queue.length === 0) {
              callback(null);
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
    var written = File.prototype.raw_write_sync.call(this, segment.s, segment.p);
    if (written != segment.l) {
      throw new Error('file.raw_write_sync returned written length (' + written + ') different from segment length (' + segment.l + ')');
    }
  }
};

BufferedFile.prototype.fetch = function(pos, length, callback) {
  if (pos >= this.queue_starts_at_pos) {
    var segment = this.segment_map[pos];
    if (segment) {
      if (segment.l != length) {
        callback(new Error('segment was found in position ' + pos + ', but length is ' + segment.l + ', not ' + length));
        return;
      } else {
        callback(null, segment.s);
      }
    } else {
      callback(new Error("position " + pos + " not found in queue"));
    }
  } else {
    File.prototype.fetch.call(this, pos, length, callback);
  }
};

BufferedFile.prototype.try_read = function(pos, length, callback) {
  var self = this;
  if (pos >= this.queue_starts_at_pos) {
    var return_string = '';
    var read_length = 0;
    var queue_pos = 0;
    var current_segment, starts_at, ends_at;
    
    while(queue_pos < this.queue.length && read_length < length) {
      current_segment = this.queue[queue_pos];
      if (pos + length >= current_segment.p && current_segment.p + current_segment.l >= pos) {
        // this segment has something we want
        starts_at = pos - current_segment.p;
        if (starts_at < 0) {
          starts_at = 0;
        }
        ends_at = pos - current_segment.p + length;
        if (ends_at > current_segment.l) {
          ends_at = current_segment.l;
        }
        var buffer = new Buffer(current_segment.s);
        if (starts_at >= 0 && ends_at > 0) {
          read_length += (ends_at - starts_at);
          var bb = buffer.toString('utf-8', starts_at, ends_at);
          return_string += bb;
        }
      }
      queue_pos ++;
    }
    callback(null, new Buffer(return_string));
  } else {
    File.prototype.try_read.call(this, pos, length, callback);
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
  this.queue = [];
  this.segment_map = {};
  this.queue_starts_at_pos = 0;
  File.prototype.clear.call(this, callback);
};

BufferedFile.prototype.written_size = function(callback) {
  if (this.write_pos) {
    callback(null, this.write_pos);
  } else {
    File.prototype.written_size.call(this, callback);
  }
  
};

File.prototype.position = function(pos) {
  this.queue_starts_at_pos = this.write_pos = pos;
};