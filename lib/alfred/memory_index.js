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