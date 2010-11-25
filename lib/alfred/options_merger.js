module.exports.merge = function(master, options) {
  
  if (!master) {
    return options;
  }
  var copy = {};
  for (var attrname in master) {
    copy[attrname] = master[attrname];
  }
  for (var attrname in options) {
    copy[attrname] = options[attrname];
  }
  return copy;
  
};