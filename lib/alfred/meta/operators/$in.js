module.exports.operateOnIndex = function(keys, index, values) {
  values.sort();
  var range_keys = index.rangeSync(values[0], values[values.length - 1]);
  var ret = range_keys.filter(function(rec) {
    for (var i = 0; i < values.length; i++) {
      if (values[i] == rec.r) {
        return true;
      }
    }
  });
  return ret;
};