module.exports.merge = function(master, options) {
  
  if (!master) {
    return options;
  }
  var copy = {};
  for (var attrname in master) {
    if (master.hasOwnProperty(attrname)) {
      copy[attrname] = master[attrname];
    }
  }
  for (var newattrname in options) {
    if (options.hasOwnProperty(newattrname)) {
      copy[newattrname] = options[newattrname];
    }
  }
  return copy;
  
};