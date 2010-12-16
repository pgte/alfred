var index_key_unitor = require('../../util/index_key_unitor');

module.exports.operateOnIndex = function(keys, index, conditions, field, finder) {
  conditions.forEach(function(condition) {
    var new_keys = finder.executeCondition(keys, field, condition);
    keys = index_key_unitor.unite(keys, new_keys);
  });
  return keys;
};