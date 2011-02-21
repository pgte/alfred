var path           = require('path'),
    fs             = require('fs'),
    util           = require('util'),
    Step           = require('step'),
    EventEmitter   = require('events').EventEmitter,
    options_merger = require('../util/options_merger'),
    cached_key_map = require('../cached_key_map'),
    replication    = require('./replication'),
    Model          = require('./model/model');

var default_meta_options = {
  meta_compact_interval: 1000 * 60 * 60, // 1 hour, +- 50%
  replication_master: false,
  replication_port: 5293,
  replication_max_file_size_kb: 10000,
  replication_max_keep_ms: 1000 * 60 * 60 * 24 * 15 // 15 days
};

var Database = function(db_path, options, callback) {
  var self = this;
  
  self.options = options_merger.merge(default_meta_options, options);
  
  path.exists(db_path, function(exists) {
    if (!exists) {
      callback(new Error('Path for database ' + path + ' does not exist'));
    } else {
      self.path = path.normalize(db_path);
      self._initialize(callback);
    }
  });
};

util.inherits(Database, EventEmitter);

module.exports.open = function(path, callback) {
  var options;
  if (arguments.length >= 3 && !!arguments[2]) {
    options = arguments[1];
    callback = arguments[2];
  }
  return new Database(path, options, callback);
};

Database.prototype._path_join = function(subpath) {
  return path.join(this.path, subpath);
};

Database.prototype._initialize = function(callback) {
  var self = this;
  Step(
    function initialize_error() {
      self.on('error', function(error) {
        if (self.listeners('error').length <= 1) {
          // if no one else is listening to error events, just throw the error into the event loop
          throw error;
        }
      });
      this();
    },
    function initialize_metadata(err) {
      if (err) {
        callback(err);
        return;
      }
      var metadata_path = self._path_join('metadata.alf');
      cached_key_map.open(metadata_path, this);
    },
    function initialize_metadata_compact(err, meta) {
      if (err) {
        callback(err);
        return;
      }
      if (self.options.meta_compact_interval > 0) {
        var compact_interval = self.options.meta_compact_interval;
        (function schedule_meta_compact() {
          // To avoid a compact storm, we schedule it randomly by compact_interval +- 50%
          var timeout = compact_interval + Math.floor(Math.random() * compact_interval) - Math.floor(compact_interval / 2);
          meta._compact_timeout = setTimeout(function() {
            if (!self._stop_compact_timeout) {
              meta.compact(function(err) {
                if (err) { self.emit('error', err); }
                schedule_meta_compact();
              });
            }
          }, timeout);
        })();
        delete self._stop_compact_timeout;

        // call compact on key_map.end
        var old_end = meta.end;
        meta.end = function(callback) {
          if (meta._compact_timeout) {
            clearTimeout(meta._compact_timeout);
            delete meta._compact_timeout;
          }
          meta._stop_compact_timeout = true;
          old_end.call(meta, callback);
        };
      }
      
      this(null, meta);
    },
    function initialize_key_maps(err, meta) {
      if (err) {
        callback(err);
      } else {
        self.meta = meta;
        self.key_maps = {};
        self.indexes = {};
        self.key_map_names = [];
        self.state = 'initializing';
        this();
      }
    },
    function read_commands_dir(err) {
      if (err) {
        callback(err);
        return;
      }
      fs.readdir(__dirname + '/commands', this);
    },
    function initialize_commands(err, command_files) {
      if (err) {
        callback(err);
        return;
      }
      command_files.forEach(function(command_file) {
        var command = require('./commands/' + command_file);
        command.register(self);
      });
      self.load(this);
    },
    function handleShutdowns(err) {
      if (err) {
        callback(err);
        return;
      }
      self.shutdown_handler = function() {
        if (self.state != 'closing' && self.state != 'closed') {
          self.closeSync();
        }
      };
      process.on('exit', self.shutdown_handler);
      this();
    },
    function start_replication(err) {
      if (err) {
        callback(err);
        return;
      }
      if (self.options.replication_master) {
        var options = {
          master: true,
          port: self.options.replication_port,
          max_file_size_kb: self.options.replication_max_file_size_kb,
          max_keep_ms: self.options.replication_max_keep_ms
        };
        self.master_replicator = replication.start(self, options, this);
        self.master_replicator.on('error', function(err) {
          self.emit('error', err);
        });
      } else {
        this(null);
      }
    },
    function finalize(err) {
      self.state = 'open';
      callback(null, self);
    }
  );
};

Database.prototype.load = function(callback) {
  var self = this;
  self._load_key_maps(function(err) {
    if (err) { callback(err); return; }
    self._load_indexes(callback);
  });
};

Database.prototype._load_key_maps = function(callback) {
  var self = this;
      
  this.meta.get('key_maps', function(err, value) {
    var key_map_name;
    
    if (err) { callback(err); return; }
    if (value) {
      var key_maps = value;
      var _key_maps = [];
      for (key_map_name in key_maps) {
        if (key_maps.hasOwnProperty(key_map_name)) {
          _key_maps.push(key_maps[key_map_name]);
        }
      }
      
      if (_key_maps.length === 0) {
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
              executed ++;
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
};

Database.prototype._load_indexes = function(callback) {
  var self = this;
  this.meta.get('indexes', function(err, indexes) {
    if (err) {
      callback(err);
      return;
    }
    if (indexes) {
      var run_indexes = [];
      for (var key_map_name in indexes) {
        if (indexes.hasOwnProperty(key_map_name)) {
          var key_map_indexes = indexes[key_map_name];
          for (var index_name in key_map_indexes) {
            if (key_map_indexes.hasOwnProperty(index_name)) {
              run_indexes.push(key_map_indexes[index_name]);
            }
          }
        }
      }
      
      if (run_indexes.length === 0) {
        callback(null);
        return;
      }
      
      run_indexes.forEach(function(run_index) {
        var key_map_indexes = indexes[key_map_name];
        var index_commands = [];
        for (var index_name in key_map_indexes) {
          if (key_map_indexes.hasOwnProperty(index_name)) {
            index_commands.push(key_map_indexes[index_name]);
          }
        }
        if (index_commands.length === 0) {
          callback(null);
        } else {
          var ran_commands = 0;
          index_commands.forEach(function(index_command) {
            self.executeCommand(index_command.command, index_command.arguments, function(err) {
              if (err) {
                callback(err);
                return;
              }
              ran_commands ++;
              if (ran_commands == index_commands.length) {
                callback(null);
              }
            });
          });
        }
      });
    } else {
      callback(null);
    }
  });
};

Database.prototype.save_key_maps = function(callback) {
  this.meta.put('key_maps', this.key_maps, callback);
};

Database.prototype.save_indexes = function(callback) {
  this.meta.put('indexes', this.indexes, callback);
};

Database.prototype.close = function(callback) {
  var self = this,
      key_map_names = [],
      key_map_name;
  
  if (self.state == 'closing') {
    callback('Database is already closing');
    return;
  }
  if (self.state == 'closed') {
    callback('Database is already closed');
    return;
  }
  self.state = 'closing';
  
  if (self.master_replicator) {
    self.master_replicator.close();
    delete self.master_replicator;
  }
  
  if (self.shutdown_handler) {
    process.removeListener('exit', self.shutdown_handler);
    delete self.shutdown_handler;
  }
  
  for (key_map_name in this.key_maps) {
    if (this.key_maps.hasOwnProperty(key_map_name)) {
      key_map_names.push(key_map_name);
    }
  }
  
  if (key_map_names.length === 0) {
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
          self.meta.end(function(err) {
            self.state = 'closed';
            callback(err);
          });
        }
      }
    });
  });
};

Database.prototype.closeSync = function() {
  var key_map_name;
  
  for (key_map_name in this.key_maps) {
    if (this.key_maps.hasOwnProperty(key_map_name)) {
      this[key_map_name].endSync();
    }
  }

  if (this.shutdown_handler) {
    process.removeListener('exit', this.shutdown_handler);
    delete this.shutdown_handler;
  }
  this.state = 'closed';
};

/* Commands */

Database.prototype.registerCommand = function(command_names, command_constructor) {
  var self = this;
      
  var invoke = function() {
    var argument_index;
    // last argument is a callback
    var command_args = [];
    for (argument_index in arguments) {
      if (argument_index < (arguments.length - 1)) {
        command_args.push(arguments[argument_index]);
      }
    }
    var command = command_constructor.apply(null, command_args);
    command.do(self, arguments[arguments.length - 1]);
  };
  if (!Array.isArray(command_names)) {
    command_names = [command_names];
  }
  command_names.forEach(function(command_name) {
    self[command_name] = invoke;
  });
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

/* Listen */

Database.prototype.listenOnAllRegisteredKeyMaps = function(callback) {
  var self = this;
  var listeners = {};
  var maps = [];
  
  this.key_map_names.forEach(function(key_map_name) {
    maps.push(key_map_name);
  });
  
  maps.forEach(function(key_map_name) {
    var listener = function(key, value) {
      callback(key_map_name, key, value);
    };
    self[key_map_name].on('put', listener);
    listeners[key_map_name] = listener;
  });
  
  self.on('key_map_attached', function(key_map_name) {
    var listener = function(key, value) {
      callback(key_map_name, key, value);
    };
    listeners[key_map_name] = listener;
    self[key_map_name].on('put', listener);
  });

  self.on('key_map_detached', function(key_map_name) {
    var listener = listeners[key_map_name];
    if (listener) {
      self[key_map_name].removeListener('put', listener);
      delete listeners[key_map_name];
    }
  });
  
  return listeners;
};

Database.prototype.stopListening = function(listeners) {
  for (var key_map_name in listeners) {
    if (listeners.hasOwnProperty(key_map_name)) {
      this[key_map_name].removeListener('put', listeners[key_map_name]);
    }
  }
};

/* Model */

Database.prototype.define = function(modelName, options) {
  return Model.define(this, modelName, options);
};