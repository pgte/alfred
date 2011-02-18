var carrier = require('carrier'),
    assert  = require('assert'),
    util             = require('util'),
    EventEmitter     = require('events').EventEmitter,
    options_merger = require('./util/options_merger');
    
var File             = require('./files/file'),
    BufferedFile     = require('./files/buffered_file'),
    FullReader       = require('./files/full_reader'),
    AlfredCheckError = require('./files/alfred_check_error');

var default_options = {
  buffered: true,
};

var Collection = function(file_path, options, callback) {
  var self = this;
  
  this.options = options_merger.merge(default_options, options);
  
  var file_class = File;
  if (this.options.buffered) {
    file_class = BufferedFile;
  }
  file_class.open(file_path, options, function(err, file) {
    if (err) {
      callback(err);
    } else {
      file.on('error', function(err) {
        self.emit('error', err);
      });
      file.on('before_flush', function() {
        self.emit('before_flush');
      });
      file.on('after_flush', function() {
        self.emit('after_flush');
      });
      self.file = file;
      callback(null, self);
    }
  });
};

util.inherits(Collection, EventEmitter);

/* collection.open(file_path [,options ], callback)  */
module.exports.open = function(file_path, callback) {
  var self = this;
  var options;
  
  if (arguments.length > 2 && arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  
  return new Collection(file_path, options, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      callback(null, coll);
    }
  });
};

Collection.prototype.encodeObject = function(object) {
  return "\n" + JSON.stringify(object) + "\n";
};

Collection.prototype.write = function(record, callback) {
  if (record === null) {
    callback(new Error("collection.write called with null record value"));
    return;
  }
  this.file.write(this.encodeObject(record), callback);
};

Collection.prototype.writeAtPos = function(record, pos, callback) {
  if (record === null) {
    callback(new Error("collection.writeAtPos called with null record value"));
    return;
  }
  this.file.rawWrite(this.encodeObject(record), pos, callback);
};

Collection.prototype.end = function(callback) {
  this.file.end(callback);
};

Collection.prototype.endSync = function() {
  return this.file.endSync();
};

Collection.prototype.clear = function(callback) {
  this.file.clear(callback);
};

Collection.prototype.destroy = function(callback) {
  this.file.destroy(callback);
};
Collection.prototype.rename = function(new_name, callback) {
  this.file.rename(new_name, callback);
};

Collection.prototype.fetch = function(pos, length, callback) {
  var self = this;
  if (!length) {
    callback(new Error("invalid length"));
  }
  
  this.file.fetch(pos, length, function(err, record_string) {
    if (err) {
      callback(err);
    } else {
      var record;
      try {
        record = JSON.parse(record_string);
      } catch (excp) {
        callback(new Error('Error parsing "' + record_string + '" at pos ' + pos + ' from file ' + self.file.file_path));
        return;
      }
      callback(null, record);            
    }
  });
  
};

Collection.prototype.read = function(callback, null_on_end) {
  var self = this;
  
  var reader = FullReader.open(this.file.file_path);
  reader.on('error', function(err) {
    if (err instanceof AlfredCheckError) {
      self.emit('warn', err);
    } else {
      callback(err);
    }
  });
  
  reader.on('warn', function(warn) {
    self.emit('warn', warn);
  });
  
  reader.on('data', function(record, pos, length) {
    callback(null, JSON.parse(record), pos, length);
  });
  
  reader.on('end', function(pos) {
    (function _do_one(){
      self.file.readOne(pos, function(err, record, length) {
        if (err) { callback(err); return; }
        if (!record) {
          callback(null, null);
        } else {
          callback(null, JSON.parse(record), pos, length);
          pos += length;
          process.nextTick(_do_one);
        }
      })
    })();
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
  }, true);
};

Collection.prototype.position = function(pos) {
  this.file.position(pos);
};