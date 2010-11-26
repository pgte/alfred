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
};

Index.prototype.each = function(callback) {
  for(var key in this.map) {
    if (this.map.hasOwnProperty(key)) {
      callback(key, this.map[key]);
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