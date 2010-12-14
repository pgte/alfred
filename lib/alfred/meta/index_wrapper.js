var IndexWrapper = function(key_map, index) {
  this.key_map = key_map;
  this.index = index;
};

module.exports.create = function(key_map, index) {
  return new IndexWrapper(key_map, index);
};

IndexWrapper.prototype.filter = function(filter_function, callback, null_on_not_found) {
  return this.key_map.filter(this.index, filter_function, callback, null_on_not_found);
};

IndexWrapper.prototype.range = function(start, end, callback) {
  return this.key_map.range(this.index, start, end, callback);
};

IndexWrapper.prototype.rangeSync = function(start, end) {
  return this.index.rangeSync(start, end);
};

IndexWrapper.prototype.count_filter = function(filter_function, callback) {
  return this.key_map.count_filter(this.index, filter_function, callback);
};

IndexWrapper.prototype.indexMatch = function(value, callback) {
  return this.key_map.indexMatch(this.index, value, callback);
};