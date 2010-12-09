var options_merger  = require('../../util/options_merger');
    
var cached_key_map  = require('../../cached_key_map'),
    indexed_key_map = require('../../indexed_key_map'),
    key_map         = require('../../key_map');

var key_map_type_map = {
  'cached_key_map' : cached_key_map,
  'indexed_key_map' : indexed_key_map,
  'key_map' : key_map
};

var default_key_map_options = {
  type: 'cached_key_map'
};

var AttachKeyMapCommand = function(key_map_name, options) {
  this.key_map_name = key_map_name;
  this.options = options_merger.merge(default_key_map_options, options);
};

var create = function(key_map_name, options) {
  return new AttachKeyMapCommand(key_map_name, options);
}

module.exports.register = function(meta) {
  meta.registerCommand('attach_key_map', create);
};

AttachKeyMapCommand.prototype.summarize = function() {
  return {command: 'attach_key_map', arguments: [this.key_map_name, this.options]};
};

AttachKeyMapCommand.prototype.do = function(meta, callback) {
  var self = this;
  var key_map_type_key = this.options.type;
  if (!key_map_type_map.hasOwnProperty(key_map_type_key)) {
    callback(new Error('Unknown key_map type: ' + key_map_type_key));
    return;
  }
  var key_map_type = key_map_type_map[key_map_type_key];
  var key_map_path = meta._path_join(this.key_map_name + '.alf');
  key_map_type.open(key_map_path, function(err, key_map) {
    if (err) {
      callback(err);
    } else {
      meta.key_maps[self.key_map_name] = self.summarize();
      meta[self.key_map_name] = key_map;
      meta.save(function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null, key_map);
      });
    }
  });
  
};