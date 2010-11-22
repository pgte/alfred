var fs = require('fs');
var carrier = require('carrier');

var open = function(file_path) {
  return fs.createWriteStream(file_path, {
    flags: 'a',
    encoding: 'utf-8'
  });
}

var Collection = function(file_path) {
  var self = this;
  this.file_path = file_path;
  this.ws = open(file_path);
  this.ws.on('error', function(error) {
    self.emit('error', error);
  });
};

(require('sys') || require('util')).inherits(Collection, require('events').EventEmitter);

module.exports.open = function(file_path) {
  return new Collection(file_path);
}

Collection.prototype.reopen = function(callback) {
  this.ws = open(this.file_path);
  if (callback) {
    callback();
  }
};

Collection.prototype.write = function(record) {
  this.ws.write(JSON.stringify(record) + "\n");
};

Collection.prototype.end = function() {
  this.ws.end();
};

Collection.prototype.prune = function(callback) {
  var self = this;
  self.end();
  fs.unlink(self.file_path, function() {
    self.reopen(callback);
  });
};

Collection.prototype.read = function(record_handler) {
  var rs = fs.createReadStream(this.file_path);
  rs.on('error', function(error) {
    record_handler(error);
  });
  rs.on('end', function() {
    rs = null;
    record_handler(null, null);
  });
  carrier.carry(rs, function(line) {
    var record = JSON.parse(line);
    record_handler(null, record);
  });
};

Collection.prototype.filter = function(filter_function, callback) {
  var records = [];
  this.read(function(error, record) {
    if (error) {
      callback(error);
    } else {
      if (record === null) { // reached the end        
        callback(null, records);
      } else {
        if (filter_function(record)) {
          records.push(record);
        }
      }
    }
  });
}