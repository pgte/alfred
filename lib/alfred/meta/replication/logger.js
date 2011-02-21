var File           = require('../../files/file'),
    fs             = require('fs'),
    path           = require('path'),
    EventEmitter   = require('events').EventEmitter,
    util           = require('util'),
    options_merger = require('../../util/options_merger');

var default_options = {
  max_file_size_kb: 10000,
  max_keep_ms: 1000 * 60 * 60 * 24 * 15 // 15 days
};
    
var Logger = function(database, options, callback) {
  var self = this;
  this.options = options_merger.merge(default_options, options);
  this.options.max_file_size = this.options.max_file_size_kb * 1024;
  this.database = database;
  this.current_log_pos = 0;
  this.livelog_file_paths = [];
  
  this.logger_id = Date.now() + Math.floor(Math.random() * 1000000000);
  
  this._clearOldBacklog(function(err) {
    if (err) { callback(err); return; }
    self._createReplicationDir(function(err) {
      if (err) { callback(err); return; }
      self._loadBacklogAndStartLiveLog(function(err) {
        if (err) { callback(err); return; }
        callback(null, self);
      });
    });
  });
};

util.inherits(Logger, EventEmitter);

module.exports.start = function(database, options, callback) {
  return new Logger(database, options, callback);
};

Logger.prototype.end = function() {
  if (this.listen_handle) {
    this.database.stopListening(this.listen_handle);
  }
};

Logger.prototype._backlogBasePath = function() {
  return this.database._path_join('replication');
};

Logger.prototype._clearOldBacklog = function(callback) {
  var baseDir = this._backlogBasePath();
  
  path.exists(baseDir, function(exists) {
    if (!exists) {
      callback(null);
      return;
    }
    fs.readdir(DB_PATH, function(err, files) {
      if (err) {callback(err); return; }
      var pendingFiles = files.length;
      if (pendingFiles === 0) {
        callback(null);
        return;
      }
      files.forEach(function(file) {
        fs.unlink(file, function(err) {
          if (err) { callback(err); return; }
          pendingFiles --;
          if (pendingFiles === 0) {
            callback(null);
          }
        });
      });
    });
  });
};

Logger.prototype._createReplicationDir = function(callback) {
  var basePath = this._backlogBasePath();
  
  path.exists(basePath, function(exists) {
    if (exists) {
      callback(null);
    } else {
      fs.mkdir(basePath, 0700, callback);
    }
  });
};

Logger.prototype._loadBacklogAndStartLiveLog = function(callback) {
  var self = this;
  
  var backLogPath = path.join(self._backlogBasePath(), 'backlog.alf');
  File.open(backLogPath, {}, function(err, backlog_file) {
    if (err) { callback(err); return; }
    
    self.backlog_file = backlog_file;
    self.livelog_file_paths.push(backLogPath);
    
    self._openLiveLog(function(err) {
      if (err) { callback(err); return; }
      self._startLiveLog(function(err) {
        if (err) { callback(err); return; }

        key_maps = self.database.key_map_names.map(function(key_map_name) {
          return {name : key_map_name, key_map: self.database[key_map_name]};
        });
        
        var keymapsLeftToBacklog = key_maps.length;
        key_maps.forEach(function(key_map) {
          self._backlogKeyMap(key_map.name, key_map.key_map, function(err) {
            if (err) { callback(err); return; }
            if (-- keymapsLeftToBacklog === 0) {
              callback(null);
            }
          });
        });
      });
    });
  });
};

Logger.prototype._backlogKeyMap = function(key_map_name, key_map, callback) {
  var self = this;
  
  var send = function(what) {
    self.current_log_pos ++;
    what.__log_pos  = self.current_log_pos;
    what.__logger_id = self.logger_id;
    self.backlog_file.write(JSON.stringify(what), function(err) {
      if (err) { callback(err); return; }
      self.emit('data', what);
    });
  };
  
  send({m: 'meta', command: 'attach_key_map', "arguments": [key_map_name, key_map.options]});
  var indexes = key_map.indexes;
  for(var index_name in indexes) {
    if (indexes.hasOwnProperty(index_name)) {
      (function(index_name) {
        var index = indexes[index_name];
        send({m: 'meta', command: 'add_index', "arguments": [key_map_name, index_name, index.options, index.transform_function.toString()]});
      })(index_name);
    }
  }
      
  key_map.collection.read(function(err, record) {
    if (err) { callback(err); return; }
    if (record !== null) {
      send({m: key_map_name, k: record.k, v: record.v});
    } else {
      callback(null);
    }
  }, true);
};

Logger.prototype._openLiveLog = function(callback) {
  var self = this;
  var pos = self.current_log_pos;
  var liveLogPath = path.join(self._backlogBasePath(), 'live_log_' + pos + '.alf');
  
  File.open(liveLogPath, {}, function(err, livelog_file) {
    if (err) { callback(err); return; }
    self.livelog_file = livelog_file;
    self.livelog_file_paths.push(liveLogPath);
    callback(null);
    self._maxKeepCheck(function(err) {
      if (err) { callback(err); return; }
    });
  });
};

Logger.prototype._startLiveLog = function(callback) {
  var self = this;
  var meta = this.database;
  
  var written = 0;
  var transitioning = false;
  var transitioningQueue = [];
  
  var send = function(what) {
    
    var reallyWrite = function(what) {
      self.current_log_pos ++;
      what.__log_pos  = self.current_log_pos;
      what.__logger_id = self.logger_id;
      self.livelog_file.write(JSON.stringify(what), function(err, pos, length) {
        if (err) { callback(err); return; }
        self.emit('data', what);
      });
    };
    
    if (written >= self.options.max_file_size) {
      transitioningQueue.push(what);
      if (!transitioning) {
        transitioning = true;
        self._openLiveLog(function(err) {
          var record;
          if (err) { callback(err); return; }
          written = 0;
          transitioning = false;
          while (transitioningQueue.length > 0) {
            record = transitioningQueue.splice(0, 1)[0];
            reallyWrite(record);
          }
        });
      }
    } else {
      reallyWrite(what);
    }
    
  };
  
  meta.on('key_map_attached', function(key_map_name) {
    send({m: 'meta', command: 'ensure_key_map_attached', "arguments": [key_map_name, meta[key_map_name].options]});
  });

  meta.on('key_map_detached', function(key_map_name) {
    send({m: 'meta', command: 'detach_key_map', "arguments": [key_map_name]});
  });

  meta.on('index_added', function(key_map_name, index_name) {
    var index = meta[key_map_name].indexes[index_name];
    send({m: 'meta', command: 'add_index', "arguments": [key_map_name, index_name, index.options, index.transform_function.toString()]});
  });

  meta.on('index_dropped', function(key_map_name, index_name) {
    send({m: 'meta', command: 'drop_index', "arguments": [key_map_name, index_name]});
  });
  
  self.listen_handle = self.database.listenOnAllRegisteredKeyMaps(function(key_map_name, key, value) {
    send({m: key_map_name, k: key, v: value});
  });
  callback(null);
};

Logger.prototype._maxKeepCheck = function(callback) {
  var self = this;
  var old = Date.now() - self.max_keep_ms;
  var files = self.livelog_file_paths.slice(0); // clone self.livelog_file_paths
  for (var i = 0; i < files.length; i++) {
    (function(i) {
      var file_path = self.livelog_file_paths[i];
      if (file_path) {
        path.exists(file_path, function(exists) {
          if (exists) {
            fs.stat(file_path, function(err, stat) {
              if (err) { callback(err); return; }
              var idx;
              if ((stat.mtime || stat.ctime).getTime() < old) {
                idx = self.livelog_file_paths.indexOf(file_path);
                if (idx >= 0) {
                  self.livelog_file_paths.splice(idx, 1);
                }
                fs.unlink(file_path, function(err) {
                  if (err) { callback(err); return; }
                });
              }
            });
          }
        });
      }
    })(i);
  }
};

Logger.prototype.nextFile = function(current_file) {
  if (!current_file) {
    return this.livelog_file_paths[0];
  }
  var idx = this.livelog_file_paths.indexOf(current_file);
  if (idx >= 0) {
    return this.livelog_file_paths[idx + 1];
  }
};

Logger.prototype.seek = function(pos) {
  var last_path, path, match, this_pos;
  
  for(var i = 0; i < this.livelog_file_paths.length; i++) {
    path = this.livelog_file_paths[i];

    match = path.match(/live_log_([0-9]+).alf$/);

    if (!match) {
      this_pos = 0;
    } else {
      this_pos = parseInt(match[1], 10);
    }
    
    if (this_pos > pos) {
      return last_path;
    }
    last_path = path;
  }
  return last_path;
};