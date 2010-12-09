var Cache = function(max_elements) {
  this.max_elements = max_elements;
  this.cache = {};
  this.element_count = 0;
  this.elements = [];
};

module.exports.open = function(max_elements) {
  return new Cache(max_elements);
};

Cache.prototype.put = function(key, value) {
  var self = this;
  this.cache[key] = value;
  process.nextTick(function() {
    var key;
    self.elements.push(key);
    self.element_count ++;
    if (self.element_count > this.max_elements) {
      key = self.elements.pop(1);
      delete self.cache[key];
      self.element_count --;
    }
  });
};

Cache.prototype.get = function(key) {
  return this.cache[key];
};

Cache.prototype.clear = function() {
  this.cache = {};
};