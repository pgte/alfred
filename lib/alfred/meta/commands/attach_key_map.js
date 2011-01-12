var options_merger  = require('../../util/options_merger');
    
var   cached_key_map  = require('../../cached_key_map')
    , indexed_key_map = require('../../indexed_key_map')
    , key_map         = require('../../key_map')
    , finder          = require('../finder');

var key_map_type_map = {
  'cached_key_map' : cached_key_map,
  'indexed_key_map' : indexed_key_map
};

var default_key_map_options = {
  type: 'cached_key_map',
  compact_interval: 1000 * 60 * 60 // 1 hour, +- 50%
};

var AttachKeyMapCommand = function(key_map_name, options) {
  this.key_map_name = key_map_name;
  this.options = options_merger.merge(default_key_map_options, options);
};

var create = function(key_map_name, options) {
  return new AttachKeyMapCommand(key_map_name, options);
}

module.exports.register = function(meta) {
  meta.registerCommand(['attach_key_map', 'attachKeyMap', 'attach'], create);
};

AttachKeyMapCommand.prototype.summarize = function() {
  return {command: 'attach_key_map', arguments: [this.key_map_name, this.options]};
};

AttachKeyMapCommand.prototype.do = function(meta, callback) {
  var self = this;
  var key_map_type_key = this.options.type;

  if (this.key_map_name == 'meta') {
    callback(new Error('Invalid key_map name: ' + this.key_map_name));
    return;
  }
  
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
      
      key_map.ensureIndex = function(name, options, transform_function, callback) {
        if (!key_map[name]) {
          key_map.addIndex(name, options, transform_function, callback);
        } else {
          callback(null, key_map[name]);
        }
      };
      
      // attach find method
      key_map.find = function(query) {
        return finder.create(key_map, query);
      };
      
      // Compact setup
      if (self.options.compact_interval > 0) {
        var compact_interval = self.options.compact_interval;
        var schedule_compact = function() {
          // To avoid a compact storm, we schedule it randomly by compact_interval +- 50%
          var timeout = compact_interval + Math.floor(Math.random() * compact_interval) - Math.floor(compact_interval / 2);
          key_map._compact_timeout = setTimeout(function() {
            if (!key_map._stop_compact_timeout) {
              key_map.compact(function(err) {
                if (err) { key_map.emit('error', err); }
                schedule_compact();
              });
            }
          }, timeout);
        };
        schedule_compact();
        delete key_map._stop_compact_timeout;

        // call compact on key_map.end
        var old_end = key_map.end;
        key_map.end = function(callback) {
          if (key_map._compact_timeout) {
            clearTimeout(key_map._compact_timeout);
            delete key_map._compact_timeout;
          }
          key_map._stop_compact_timeout = true;
          old_end.call(key_map, callback);
        };
      }
      
      // add error listener
      key_map.on('error', function(err) {
        meta.emit('error', err);
      });
      
      // Update meta
      meta.key_maps[self.key_map_name] = self.summarize();
      meta[self.key_map_name] = key_map;
      meta.key_map_names.push(self.key_map_name);
      meta.save_key_maps(function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null, key_map);
        meta.emit('key_map_attached', self.key_map_name);
      });
    }
  });
  
};