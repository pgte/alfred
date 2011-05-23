var Script = process.binding('evals').Script || process.binding('evals').NodeScript;

var index_wrapper = require('../index_wrapper');

var AddIndexCommand = function(key_map_name, name, options, transform_function) {
  this.key_map_name = key_map_name;
  this.name = name;
  this.options = options;
  if (typeof transform_function == 'function') {
    transform_function = transform_function.toString();
  }
  transform_function = 'module.exports = ' + transform_function + ';';
  var context = {module: {}};
  Script.runInNewContext(transform_function, context);
  this.transform_function = context.module.exports;
};

var create = function(key_map_name, name, options, transform_function) {
  return new AddIndexCommand(key_map_name, name, options, transform_function);
}

module.exports.register = function(meta) {
  meta.registerCommand(['add_index', 'addIndex'], create);
};

AddIndexCommand.prototype.summarize = function() {
  return {command: 'add_index', arguments: [this.key_map_name, this.name, this.options, this.transform_function.toString()]};
};

AddIndexCommand.prototype.do = function(meta, callback) {
  
  var self = this;
  var key_map = meta[this.key_map_name];
  
  if (!key_map) {
    callback(new Error('KeyMap with name ' + this.key_map_name + ' was not found'));
    return;
  }
  
  if (key_map[this.name]) {
    callback(null, key_map[this.name]);
  }
  
  key_map._addIndex(this.name, this.options, this.transform_function, function(err, index) {
    if (err) {
      callback(err);
      return;
    }
    var index_wrpr = index_wrapper.create(key_map, index);
    if (!meta.indexes[self.key_map_name]) {
      meta.indexes[self.key_map_name] = {};
    }
    key_map[self.name] = index_wrpr;
    meta.indexes[self.key_map_name][self.name] = self.summarize();
    meta.save_indexes(function(err) {
      if (err) {
        callback(err);
        return;
      }
      meta.emit('index_added', self.key_map_name, self.name);
      callback(null);
    });
    
  });
  
};
