var EventEmitter     = require('events').EventEmitter;
var FinderStream = function(query, finder) {
  var self = this;
  this.query = query;
  this.keys = null;
  this.finder = finder;
  this.collection = finder.key_map.collection;
  this.paused = false;
  this.ended = false;
  process.nextTick(function() {
    self.watch();
  });
};

require('util').inherits(FinderStream, EventEmitter);

module.exports.open = function(query, finder) {
  return new FinderStream(query, finder);
};

FinderStream.prototype.pause = function() {
  this.paused = true;
};

FinderStream.prototype.resume = function() {
  var self = this;
  this.paused = false;
  process.nextTick(function() {
    self.watch();
  });
};

FinderStream.prototype.watch = function() {
  var self = this;
  (function _watch() {
    if (self.paused) {
      return;
    }
    if (! self.keys) {
      self.finder.executeAllAndJustReturnKeysInOrder(self.query, function(err, keys) {
        if (err) {
          self.emit('error', err);
          return
        }
        self.keys = keys;
        process.nextTick(function() {
          _watch();
        });
      })
    } else {
      if (self.keys.length > 0) {
        key = self.keys.splice(0, 1)[0];
        self.collection.fetch(key.p, key.l, function(err, record) {
          self.emit('record', record.k, record.v);
          process.nextTick(function() {
            _watch();
          });
        });
      } else {
        if (!self.ended) {
          self.emit('end');
        }
      }
    }
  })();
};