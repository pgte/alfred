var DropIndexCommand = function(key_map_name, name, options) {
  this.key_map_name = key_map_name;
  this.name = name;
};

var create = function(key_map_name, name) {
  return new DropIndexCommand(key_map_name, name);
}

module.exports.register = function(meta) {
  meta.registerCommand(['drop_index', 'dropIndex'], create);
};

DropIndexCommand.prototype.summarize = function() {
  return {command: 'drop_index', arguments: [this.key_map_name, this.name]};
};

DropIndexCommand.prototype.do = function(meta, callback) {
  var self = this;
  var key_map = meta[this.key_map_name];
  if (!key_map) {
    callback(new Error('KeyMap with name ' + this.key_map_name + ' was not found'));
    return;
  }
  
  key_map.dropIndex(this.name, function(err) {
    if (err) {
      self.callback(err);
      return;
    }
    if (!meta.indexes[self.key_map_name]) {
      callback(new Error('Key map with name ' + self.key_map_name + ' not found'));
      return;
    }
    delete key_map[self.name];
    delete meta.indexes[self.key_map_name][self.name];
    meta.save_indexes(function(err) {
      if (err) {
        callback(err);
        return;
      }
      meta.emit('index_dropped', self.key_map_name, self.name);
      callback(null);
    });
  });
  
};