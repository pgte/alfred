var util = require('util');

var Index = function() {
  this.map = {};
  this.waitQueue = [];
};

module.exports.open = function() {
  return new Index();
};

Index.prototype.put = function(key, offset, length, callback, secret) {
  
  var self = this;

  if (offset === undefined || offset === null) { throw new Error('invalid offset: ' + util.inspect(offset))}
  if (!length) { throw new Error('invalid length: ' + util.inspect(length))}
  if (!callback) { throw new Error('invalid callback: ' + util.inspect(callback))}

  this.waitForAtomic(key, secret, function(err, rec) {
    if (err) {
      callback(err);
    } else {
      if (!rec) {
        self.map[key] = {o: offset, l: length};
      } else {
        rec.o = offset;
        rec.l = length;
      }
      callback(null);
    }
  });

};

Index.prototype.get = function(key, callback) {

  this.waitForAtomic(key, null, function(err, rec) {
    if (err) {
      callback(err);
    } else {
      if (!rec) {
        callback(null, null);
      } else {
        callback(null, rec);
      }
    }
  });

};

Index.prototype.clear = function() {
  this.map = {};
};

Index.prototype.each = function(callback) {
  var self = this;
  for(var key in this.map) {
    if (this.map.hasOwnProperty(key)) {
      (function(key) {
        self.get(key, function(err, record) {
          if (err) {
            callback(err);
          } else {
            callback(null, key, record.o, record.l);
          }
        });
      })(key);
    }
  }
};

Index.prototype.count = function() {
  var count = 0;
  for(var key in this.map) {
    if (this.map.hasOwnProperty(key)) {
      count ++;
    }
  }
  return count;
};

Index.prototype.size = function() {
  var size = 0;
  for(var key in this.map) {
    if (this.map.hasOwnProperty(key)) {
      var rec = this.map[key];
      size += rec.l + 6;
    }
  }
  return size;
};

/** Clone **/

Index.prototype.clone = function() {
  var new_index = new Index();
  new_index.map = {};
  for(var key in this.map) {
    if (this.map.hasOwnProperty(key)) {
      new_index.map[key] = this.map[key];
    }
  }
  return new_index;
};

/** Atomic **/

Index.prototype.wait = function(key, callback) {
  this.waitQueue.push({k:key,c:callback});
};

Index.prototype.processWaiting = function(callback) {
  var self = this;
  process.nextTick(function() {
    var waiting;
    var cb;
    while (self.waitQueue.length > 0) {
      waiting = self.waitQueue.splice(0, 1)[0];
      (function(waiting) {
        cb = waiting.c;
        self.get(waiting.k, function(err, record) {
          if (err) {
            cb(err);
          } else {
            cb(null, record);
          }
        });
      })(waiting);
    }
  });
};

Index.prototype.waitForAtomic = function(key, secret, callback) {
  var self = this;
  var rec = this.map[key];
  if (rec && rec.a) {
    if (secret) {
      if (secret == rec.s) {
        callback(null, rec);
      } else {
        callback(new Error('secret ' + secret + ' does not match'));
      }
    } else {
      self.wait(key, function(err) {
        if (err) {
          callback(err);
        } else {
          callback(null, rec);
        }
      });
    }
  } else {
    callback(null, rec);
  }
};

Index.prototype.atomic = function(key, secret, callback) {
  var self = this;
  self.waitForAtomic(key, secret, function(err, rec) {
    if (err) {
      callback(err);
    } else {
      var free = function() {
        if (rec) {
          delete rec.a;
          delete rec.s;
        }
        self.processWaiting();
      };
      if (!secret) {
        secret = Math.floor(Math.random() * 99999999999999999999);
      }
      if (rec) {
        rec.a = true;
        rec.s = secret;
        callback(null, secret, rec, free);
      } else {
        callback(null, secret, null, free);
      }
    }
  });
};