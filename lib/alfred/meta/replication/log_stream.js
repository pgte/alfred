var EventEmitter   = require('events').EventEmitter,
    assert         = require('assert'),
    util           = require('util');

var LogStream = function(logger) {
  var self = this;
  
  this.logger = logger;
  this.current_file_index = -1;
  this.paused = false;
  this.current_file_pos = 0;
  
  process.nextTick(function() {
    self._read();
  });
};

util.inherits(LogStream, EventEmitter);

module.exports.open = function(logger) {
  return new LogStream(logger);
};

LogStream.prototype._grabNextFile = function(callback) {
  var self = this;
  this.current_file_index ++;
  this.current_file = this.logger.livelog_files[this.current_file_index];
  this.current_file_pos = 0;
  if (!this.current_file) {
    this.logger.once('data', function() {
      self.current_file = self.logger.livelog_files[self.current_file_index]
      assert.ok(!!self.current_file, 'Couldn\'t get a file from the logger');
      callback(self.current_file)
    });
  } else {
    callback(this.current_file)
  }
  
}

LogStream.prototype._read = function() {
  var self = this;
  if (this.paused) { return; }
  if (!this.current_file) {
    this._grabNextFile(function(file) {
      self._read();
    });
  } else {
    this.current_file.readOne(this.current_file_pos, function(err, record, length) {
      if (err) { self.emit('error', err); return; }
      if (record) {
        self.current_file_pos += length;
        self.emit('data', record + "\n");
        self._read();
      } else {
        // file reached end
        // now we have to know if it has rolled into another file or should we keep trying to read this one
        // check if a new file exists
        if (self.logger.livelog_files.length > (self.current_file_index + 1)) {
          self._grabNextFile(function() {
            process.nextTick(function() {
              self._read();
            })
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
}

LogStream.prototype.resume = function() {
  if (!this.paused) { return; }
  this.paused = false;
  this._read();
}

LogStream.prototype.destroy = function() {
  delete self.logger;
  delete self.current_file_index;
  delete self.current_file;
};