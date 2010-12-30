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

IndexWrapper.prototype.filterSync = function(filter_function, callback, null_on_not_found) {
  return this.index.filterSync(filter_function);
};

IndexWrapper.prototype.range = function(start, end, callback) {
  return this.key_map.range(this.index, start, end, callback);
};

IndexWrapper.prototype.rangeSync = function(start, end, exclusive_start, exclusive_end) {
  return this.index.rangeSync(start, end, exclusive_start, exclusive_end);
};

IndexWrapper.prototype.countFilter = function(filter_function, callback) {
  return this.key_map.countFilter(this.index, filter_function, callback);
};

IndexWrapper.prototype.indexMatch = function(value, callback) {
  return this.key_map.indexMatch(this.index, value, callback);
};

IndexWrapper.prototype.indexMatchSync = function(value) {
  return this.index.matchSync(value);
};