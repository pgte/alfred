module.exports.operateOnIndex = function(keys, index, value) {
  return index.filterSync(function(rec) {
    return rec != value;
  });
};