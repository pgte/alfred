var EventEmitter   = require('events').EventEmitter,
    assert         = require('assert'),
    util           = require('util'),
    File           = require('../../files/file');

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
  var file_path = this.logger.livelog_file_paths[this.current_file_index];
  var openFilePath = function(file_path) {
    File.open(file_path, {read_only: true}, function(err, file) {
      if (err) { callback(err); return; }
      self.current_file = file;
      self.current_file_pos = 0;
      callback(self.current_file)
    })
  };
  if (!file_path) {
    self.logger.once('data', function() {
      file_path = this.logger.livelog_file_paths[this.current_file_index];
      assert.ok(!!file_path, 'Couldn\'t get a file from the logger');
      openFilePath(file_path);
    });
  } else {
    openFilePath(file_path);
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
        if (self.logger.livelog_file_paths.length > (self.current_file_index + 1)) {
          self.current_file.end(function(err) {
            if (err) { emit('error', err); return; }
          });
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