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
  
  if (meta[this.key_map_name]) {
    callback(new Error(this.key_map_name + ' is already attached'));
    return;
  }
  
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
      
      // Add Index
      key_map._addIndex = key_map.addIndex;
      key_map.addIndex = function(name, transform_function, callback) {
        var options;
        if (arguments.length == 4 && !!arguments[3]) {
          options = arguments[1];
          transform_function = arguments[2];
          callback = arguments[3];
        }
        meta.add_index(self.key_map_name, name, options, transform_function, callback);
      };
      
      // Update meta
      meta.key_maps[self.key_map_name] = self.summarize();
      meta[self.key_map_name] = key_map;
      meta.save_key_maps(function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null, key_map);
      });
    }
  });
  
};