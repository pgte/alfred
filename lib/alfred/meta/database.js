var path = require('path'),
    fs   = require('fs'),
    Step = require('step');

var cached_key_map  = require('../cached_key_map');

var Database = function(db_path, callback) {
  var self = this;
  path.exists(db_path, function(exists) {
    if (!exists) {
      callback(new Error('Path for database ' + path + ' does not exist'));
    } else {
      self.path = path.normalize(db_path);
      self._initialize(callback);
    }
  });
};

module.exports.open = function(path, callback) {
  return new Database(path, callback);
};

Database.prototype._path_join = function(subpath) {
  return path.join(this.path, subpath);
};

Database.prototype._initialize = function(callback) {
  var self = this;
  Step(
    function initialize_metadata() {
      var metadata_path = self._path_join('metadata.alf');
      cached_key_map.open(metadata_path, this);
    },
    function initialize_key_maps(err, meta) {
      if (err) {
        throw err;
      } else {
        self.meta = meta;
        self.key_maps = {};
        this();
      }
    },
    function read_commands_dir() {
      fs.readdir(__dirname + '/commands', this);
    },
    function initialize_commands(err, command_files) {
      try {
        command_files.forEach(function(command_file) {
          var command = require('./commands/' + command_file);
          command.register(self);
        });
      } catch(excp) {
        callback(excp);
        return;
      }
      self.load(this);
    },
    function finalize(err) {
      callback(err, self);
    }
  );
};

Database.prototype.load = function(callback) {
  var self = this;
  this.meta.get('key_maps', function(err, value) {
    if (err) {
      callback(err);
      return;
    }
    if (value) {
      var key_maps = value;
      var _key_maps = [];
      for (key_map_name in key_maps) {
        if (key_maps.hasOwnProperty(key_map_name)) {
          _key_maps.push(key_maps[key_map_name]);
        }
      }
      
      if (_key_maps.length == 0) {
        callback(null);
        return;
      }
      
      var executed = 0;
      _key_maps.forEach(function(key_map) {
        (function(key_map) {
          self.executeCommand(key_map.command, key_map['arguments'], function(err) {
            if (err) {
              callback(err);
            } else {
              executed ++
              if (executed == _key_maps.length) {
                callback(null);
              }
            }
          });
        })(key_map);
        
      });
    } else {
      callback(null);
    }
  });
}

Database.prototype.save = function(callback) {
  this.meta.put('key_maps', this.key_maps, callback);
};

Database.prototype.close = function(callback) {
  var self = this;
  var key_map_names = []
  
  for (key_map_name in this.key_maps) {
    if (this.key_maps.hasOwnProperty(key_map_name)) {
      key_map_names.push(key_map_name);
    }
  }
  
  if (key_map_names.length == 0) {
    self.meta.end(callback);
    return;
  }
  
  var done = 0;
  key_map_names.forEach(function(key_map_name) {
    var key_map = self[key_map_name];
    key_map.end(function(err) {
      if (err) {
        callback(err);
      } else {
        delete self[key_map_name];
        done ++;
        if (done == key_map_names.length) {
          self.meta.end(callback);
        }
      }
    });
  });
};

/* Commands */

Database.prototype.registerCommand = function(command_name, command_constructor) {
  var self = this;
  this[command_name] = function() {
    // last argument is a callback
    var command_args = [];
    for (argument_index in arguments) {
      if (argument_index < (arguments.length - 1)) {
        command_args.push(arguments[argument_index])
      }
    }
    var command = command_constructor.apply(null, command_args);
    command.do(self, arguments[arguments.length - 1]);
  };
};

Database.prototype.executeCommand = function(command_name, arguments, callback) {
  var command = this[command_name];
  if (!command) {
    callback(new Error('Command ' + command_name + ' not found'));
    return;
  }
  arguments.push(callback);
  command.apply(this, arguments);
};