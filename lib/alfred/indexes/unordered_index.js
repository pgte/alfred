var UnorderedIndex = function(transform_function) {  
  this.transform_function = transform_function;
  this.records = [];
  this.map = {};
};

module.exports.open = function(transform_function) {
  return new UnorderedIndex(transform_function);
};

UnorderedIndex.prototype.transformFunction = function() {
  return this.transform_function;
};

UnorderedIndex.prototype.put = function(key, value, pos, length) {
  if (value) {
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
  } else {
    var pos = this.map[key];
    if (pos !== undefined) {
      this.records[pos] = null;
    } else {
    }
    delete this.map[key];
  }
};

UnorderedIndex.prototype.match = function(match, callback) {
  var match_function = function(rec) {
    return rec == match;
  };
  this.filter(match_function, callback, true);
};

UnorderedIndex.prototype.filter = function(filter_function, callback, null_on_not_found){
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

UnorderedIndex.prototype.count = function(filter_function, callback) {
  var count = 0;
  this.records.forEach(function(record) {
    if (record && filter_function(record.r)) {
      count ++;
    }
  });
  callback(null, count);
};
