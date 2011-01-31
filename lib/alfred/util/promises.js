var Promises = function() {
  this.pendingCount = 0;
  this.doneCallbacks = [];
};

module.exports.new = function() {
  return new Promises();
};

Promises.prototype.add = function(promise) {
  var self = this, cb;
  self.pendingCount ++;
  promise(function() {
    self.pendingCount --;
    if (self.pendingCount === 0) {
      while (self.doneCallbacks.length > 0) {
        cb = self.doneCallbacks.splice(0, 1)[0];
        cb();
      }
    }
  });
};

Promises.prototype.done = function(callback) {
  if (this.pendingCount === 0) {
    callback();
  } else {
    this.doneCallbacks.push(callback);
  }
};