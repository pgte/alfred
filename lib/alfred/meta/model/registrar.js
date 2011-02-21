var modelsByName = {};

module.exports.put = function(name, model) {
  if (modelsByName[name]) {
    throw new Error('Model with name ' + name + ' already defined');
  }
  modelsByName[name] = model;
};

module.exports.get = function(name) {
  return modelsByName[name];
};