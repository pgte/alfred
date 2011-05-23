var EventEmitter   = require('events').EventEmitter,
    assert         = require('assert'),
    util           = require('util'),
    File           = require('../../files/file');

var LogStream = function(logger, from_pos) {
  var self = this;
  
  this.logger = logger;
  
  if (!logger) {
    console.log('no logger on LogStream');
    throw new Exception('no logger on LogStream');
  }
  
  this.paused = false;
  this.current_file_pos = 0;
  
  process.nextTick(function() {
    if (from_pos && from_pos > 0) {
      self._seek(from_pos, function(err) {
        if (err) { self.emit('error', err); }
        self._read();
      });
    } else {
        self._read();
    }
  });
};

util.inherits(LogStream, EventEmitter);

module.exports.open = function(logger, from_pos) {
  return new LogStream(logger, from_pos);
};

LogStream.prototype._grabNextFile = function(callback) {
  var self = this;
  var file_path = self.logger.nextFile(self.current_file && self.current_file.file_path);
  var openFilePath = function(file_path) {
    File.open(file_path, {read_only: true}, function(err, file) {
      if (err) { callback(err); return; }
      self.current_file = file;
      self.current_file_pos = 0;
      callback(self.current_file);
    });
  };
  if (!file_path) {
    self.logger.once('data', function() {
      file_path = self.logger.nextFile(self.current_file && self.current_file.file_path);
      assert.ok(!!file_path, 'Couldn\'t get a file from the logger');
      openFilePath(file_path);
    });
  } else {
    openFilePath(file_path);
  }
};

LogStream.prototype._seek = function(pos, callback) {
  var self = this;
  var rec;
  
  delete self.current_file;
  self.current_file_pos = 0;
  File.open(self.logger.seek(pos), {read_only: true}, function(err, file) {
    if (err) { callback(err); return; }
    self.current_file = file;
    (function inside_seek() {
      self.current_file.readOne(self.current_file_pos, function(err, record, length) {
        if (err) { callback(err); return; }
        if (!record) {
          callback(null);
          return;
        }
        rec = JSON.parse(record);
        self.current_file_pos += length;
        if (pos >= rec.__log_pos) {
          process.nextTick(inside_seek);
        } else {
          self.current_file_pos -= length;
          callback(null);
        }
      });
    })();
  });
};

LogStream.prototype._read = function() {
  var self = this;
  if (self.paused) { return; }
  if (!self.current_file) {
    self._grabNextFile(function(file) {
      self._read();
    });
  } else {
    self.current_file.readOne(self.current_file_pos, function(err, record, length) {
      if (err) { self.emit('error', err); return; }
      if (record) {
        self.current_file_pos += length;
        self.emit('data', record + "\n");
        self._read();
      } else {
        // file reached end
        // now we have to know if it has rolled into another file or should we keep trying to read this one
        // check if a new file exists
        if (self.logger.nextFile(self.current_file && self.current_file.file_path)) {
          self.current_file.end(function(err) {
            if (err) { emit('error', err); return; }
          });
          self._grabNextFile(function() {
            process.nextTick(function() {
              self._read();
            });
          });
        } else {
          // if not, wait for new data
          self.logger.once('data', function() {
            process.nextTick(function() {
              self._read();
            });
          });
        }
      }
    });
  }
};

LogStream.prototype.pause = function() {
  this.paused = true;
};

LogStream.prototype.resume = function() {
  if (!this.paused) { return; }
  this.paused = false;
  this._read();
};

LogStream.prototype.destroy = function() {
  //delete this.logger;
  delete this.current_file_index;
  delete this.current_file;
};