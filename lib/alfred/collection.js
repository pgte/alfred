var fs      = require('fs'),
    carrier = require('carrier'),
    assert  = require('assert'),
    path    = require('path');

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
  fs.open(self.file_path, 'a+', 0666, function(err, fd) {
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