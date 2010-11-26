var FunctionalIndex = function(transform_function) {  
  this.transform_function = transform_function;
  this.records = [];
  this.map = {};
};

module.exports.open = function(transform_function) {
  return new FunctionalIndex(transform_function);
};

FunctionalIndex.prototype.put = function(key, value, pos, length) {
  var transformed = this.transform_function(value);
  if (transformed) {
    var newRecord = {
      r: transformed,
      k: key,
      p: pos,
      l: length
    };
    var old_pos = this.map[key];
    if (old_pos) {
      this.records[old_pos] = newRecord;
    } else {
      this.map[key] = this.records.length;
      this.records.push(newRecord);
    }
  }
};

FunctionalIndex.prototype.filter = function(filter_function, callback, null_on_not_found){
  var not_found = true;
  this.records.forEach(function(record) {
    if (record && filter_function(record.r)) {
      not_found = false;
      callback(record.k, record.p, record.l);
    }
  });
  if (null_on_not_found && not_found) {
    callback(null);
  }
};

FunctionalIndex.prototype.count = function(filter_function, callback) {
  var count = 0;
  this.records.forEach(function(record) {
    if (record && filter_function(record.r)) {
      count ++;
    }
  });
  callback(null, count);
};