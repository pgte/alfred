var index_wrapper = require('../index_wrapper');

var AddIndexCommand = function(key_map_name, name, options, transform_function, callback) {
  this.key_map_name = key_map_name;
  this.name = name;
  this.options = options;
  this.transform_function = transform_function;
  this.callback = callback;
};

var create = function(key_map_name, name, options, transform_function, callback) {
  return new AddIndexCommand(key_map_name, name, options, transform_function, callback);
}

module.exports.register = function(meta) {
  meta.registerCommand('add_index', create);
};

AddIndexCommand.prototype.summarize = function() {
  return {command: 'add_index', arguments: [this.key_map_name, this.name, this.options, this.transform_function]};
};

AddIndexCommand.prototype.do = function(meta, callback) {
  var self = this;
  var key_map = meta[this.key_map_name];
  if (!key_map) {
    callback(new Error('KeyMap with name ' + this.key_map_name + ' was not found'));
    return;
  }
  
  key_map._addIndex(this.name, this.options, this.transform_function, function(err, index) {
    var index_wrpr = index_wrapper.create(key_map, index);
    if (err) {
      self.callback(err);
      return;
    }
    if (!meta.indexes[self.key_map_name]) {
      meta.indexes[self.key_map_name] = {};
    }
    key_map[self.name] = index_wrpr;
    meta.indexes[self.key_map_name][self.name] = self.summarize();
    meta.save(callback);
  });
  
};