var net                = require('net'),
    fs                 = require('fs'),
    carrier            = require('carrier'),
    util               = require('util'),
    EventEmitter       = require('events').EventEmitter,
    ReplicationLogger  = require('./logger');

var commands = 
  (function() {
    var commands = fs.readdirSync(__dirname + '/commands');
    var commandModules = {};
    commands.forEach(function(command) {
      var commandModuleName = command.replace(/\.js$/, '');
      commandModules[commandModuleName] = require('./commands/' + command);
    });
    return commandModules;
  })();

var options_merger = require('../../util/options_merger');

var default_options = {
  port: 5293,
  max_file_size_kb: 10000
};

var Master = function(database, options, callback) {
  this.database = database;
  this.options = options_merger.merge(default_options, options);
  this._initialize(callback);
};

util.inherits(Master, EventEmitter);

module.exports.start = function(database, options, callback) {
  return new Master(database, options, callback);
};

Master.prototype._initialize = function(callback) {
  this._initializeServer(callback);
};

Master.prototype._initializeServer = function(callback) {
  var self = this;
  
  this.server = net.createServer(function(stream) {
    
    try {
      stream.on('error', function(err) {
        self.emit('error', err);
      });
    
      carrier.carry(stream, function(command_line) {
        command_line = command_line.trim();
        if (command_line.length > 0) {
          try {
            command = JSON.parse(command_line);
            self._executeCommand(command, stream);
          } catch (excep) {
            self.error(stream, excep);
            return;
          }
        }
      });
    } catch (excep) {
      self.error(stream, excep);
    }
  });
  this.server.listen(this.options.port);
  callback(null);
};

Master.prototype._executeCommand = function(command, stream) {
  var self = this;
  this.loggerStarted(function(err) {
    if (err) { self.error(stream, err); return; }
    
    try {
      if (!command) {
        throw new Error('No command sent');
      }
      if (!command.command) {
        throw new Error('No command.command sent');
      }
      var command_function = commands[command.command];
      if (!command_function || typeof(command_function) != 'function' ) {
        throw new Error('unrecognized command ' + command.command);
      }
      command_function(self, command['arguments'], stream);
    } catch(exception) {
      self.error(stream, exception);
    }
    
  });
};

Master.prototype.error = function(stream, error) {
  stream.write(JSON.stringify({error: error.message}), 'utf8');
};

Master.prototype.close = function() {
  if (this.logger) {
    this.logger.end();
  }
  this.server.close();
};

/* Logger */

Master.prototype.loggerStarted = function(callback) {
  var self = this;
  if (this.logger) {
    callback(null);
  } else {
    ReplicationLogger.start(self.database, {max_file_size_kb: self.options.max_file_size_kb}, function(err, logger) {
      if (err) { callback(err); return; }
      self.logger = logger;
      callback(null);
    });
  }
};