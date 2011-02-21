var DetachKeyMapCommand = function(key_map_name) {
  this.key_map_name = key_map_name;
};

var create = function(key_map_name) {
  return new DetachKeyMapCommand(key_map_name);
}

module.exports.register = function(meta) {
  meta.registerCommand(['detach_key_map', 'detachKeyMap', 'detach'], create);
};

DetachKeyMapCommand.prototype.summarize = function() {
  return {command: 'detach_key_map', arguments: [this.key_map_name]};
};

DetachKeyMapCommand.prototype.do = function(meta, callback) {
  var self = this;
  var key_map = meta[this.key_map_name];
  
  if (!key_map) {
    callback(new Error('Key map with name ' + this.key_map_name + ' not found'));
    return;
  }
  key_map.end(function(err) {
    if (err) {
      callback(err);
      return;
    }
    delete meta.key_maps[self.key_map_name];
    delete meta[self.key_map_name];
    // remove keymap name
    var key_map_pos = meta.key_map_names.indexOf(self.key_map_name);
    if (key_map_pos >= 0) {
      meta.key_map_names.splice(key_map_pos, 1);
    }
    
    meta.save_key_maps(function(err) {
      if (err) {
        callback(err);
        return;
      }
      callback(null);
      
      meta.emit('key_map_detached', self.key_map_name);
      
    });
  });
};