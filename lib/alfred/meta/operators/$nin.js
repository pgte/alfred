module.exports.operateOnIndex = function(keys, index, values) {
  return index.filterSync(function(rec) {
    for (var i = 0; i < values.length; i++) {
      if (rec == values[i]) {
        return false;
      }
    }
    return true;
  });
};