var Index = function() {
  this.map = {};
};

module.exports.open = function() {
  return new Index();
};

Index.prototype.put = function(key, offset, length) {
  this.map[key] = {offset: offset, length: length};
};

Index.prototype.get = function(key) {
  return this.map[key];
};