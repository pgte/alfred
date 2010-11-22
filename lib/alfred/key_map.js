var collection = require('./collection');

var KeyMap = module.exports.class = function(file_path, callback) {
  this.file_path = file_path;
  var self = this;
  collection.open(file_path, function(err, coll) {
    if (err) {
      callback(err);
    } else {
      self.collection = coll;
      callback(null, self);
    }
  });
  
};

module.exports.open = function(file_path, callback) {
  return new KeyMap(file_path, callback);
};

KeyMap.prototype.put = function(key, value, callback) {
  this.collection.write({
    key: key,
    value: value,
    created: (new Date()).getTime()
  },
  callback);
};

KeyMap.prototype.get = function(key, callback) {
  var value = null;
  this.collection.filter(function(record) {
    return record.key === key;
  }, function(error, record) {
    if (error) {
      callback(error);
      return;
    }
    if (record === null) {
      callback(null, value);
    } else {
      value = record.value;
    }
  });
};

KeyMap.prototype.get_at_pos = function(pos, length, callback) {
  if (!length) {
    callback(new Error("invalid length"));
  }
  
  this.collection.fetch(pos, length, function(err, record) {
    if (err) {
      callback(err);
    } else {
      if (record) {
        callback(null, record.key, record.value);
      } else {
        callback(null, null);
      }
    }
  });
};

KeyMap.prototype.clear = function(callback) {
  this.collection.clear(callback);
};

KeyMap.prototype.each = function(callback) {
  this.collection.read(function(err, record) {
    if (err) {
      callback(err);
    } else {
      if (record) {
        callback(null, record.key, record.value);
      }
    }
  });
}

KeyMap.prototype.each_with_pos = function(callback) {
  this.collection.read_with_pos(function(err, record, pos, length) {
    if (err) {
      callback(err);
    } else {
      if (record) {
        callback(null, record.key, record.value, pos, length);
      }
    }
  });
}