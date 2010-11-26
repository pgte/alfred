var Index = function() {
  this.map = {};
};

module.exports.open = function() {
  return new Index();
};

Index.prototype.put = function(key, offset, length) {
  this.map[key] = {o: offset, l: length};
};

Index.prototype.get = function(key) {
  return this.map[key];
};

Index.prototype.clear = function() {
  this.map = {};
}

Index.prototype.each = function(callback) {
  for(key in this.map) {
    callback(key, this.map[key]);
  }
};

Index.prototype.count = function() {
  var count = 0;
  for(key in this.map) {
    count ++;
  }
  return count;
};