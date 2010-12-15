var index_key_interceptor = require('../../util/index_key_interceptor');

module.exports.operateOnIndex = function(keys, index, value) {
  var new_keys = index.rangeSync(value, null, true);
  return index_key_interceptor.intercept(keys, new_keys);
};