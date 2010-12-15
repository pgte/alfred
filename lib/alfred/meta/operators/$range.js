var index_key_interceptor = require('../../util/index_key_interceptor');

module.exports.operateOnIndex = function(keys, index, value) {
  var new_keys = index.rangeSync(value[0], value[1], value[2], value[3]);
  return index_key_interceptor.intercept(keys, new_keys);
};