var File         = require('../../files/file'),
    fs           = require('fs'),
    path         = require('path'),
    EventEmitter = require('events').EventEmitter,
    util         = require('util');

var Logger = function(database, callback) {
  var self = this;
  this.database = database;
  this.current_log_pos = 0;
  this.livelog_files = [];
  this._clearOldBacklog(function(err) {
    if (err) { callback(err); return; }
    self._createReplicationDir(function(err) {
      if (err) { callback(err); return; }
      self._loadBacklogAndStartLiveLog(function(err) {
        if (err) { callback(err); return; }
        callback(null, self);
      });
    })
  });
};

util.inherits(Logger, EventEmitter);

module.exports.start = function(database, callback) {
  return new Logger(database, callback);
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
    self.livelog_files.push(backlog_file);
    
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
    self.livelog_files.push(self.livelog_file);
    callback(null);
  });
};

Logger.prototype._startLiveLog = function(callback) {
  var self = this;
  var meta = this.database;
  
  var send = function(what) {
    self.livelog_file.write(JSON.stringify(what), function(err) {
      if (err) { callback(err); return; }
      self.emit('data', what);
    });
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