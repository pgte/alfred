var options_merger = require('../../util/options_merger');
var net     = require('net'),
    carrier = require('carrier');

var default_options = {
  source_port: 5293,
  reconnect_count: 20,
  reconnect_timeout: 5000
};

var ReplicateFromCommand = function(source, options) {
  this.source = source;
  this.options = options_merger.merge(default_options, options);
};

var create = function(source, options) {
  return new ReplicateFromCommand(source, options);
}

module.exports.register = function(meta) {
  meta.registerCommand('replicate_from', create);
};

ReplicateFromCommand.prototype.do = function(meta, callback) {
  var self = this;
  var connection;
  var reconnect_tries;
  
  var command_queue = [];
  
  var in_notify = false;

  var notify = function() {
    if (in_notify) {
      return;
    }
    in_notify = true;
    (function do_notify() {
      if (command_queue.length < 1) {
        return;
      }
      var command = command_queue.splice(0, 1)[0];
      do_command(command, function() {
        process.nextTick(function() {
          in_notify = false;
          do_notify();
        });
      });
    })();
  };

  var do_command = function(command, callback) {
    var key_map_name = command.m;

    if (key_map_name == 'meta') {
      meta.executeCommand(command.command, command.arguments, function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null);
      });
    } else {
      var key = command.k;
      var value = command.v;
      var key_map = meta[key_map_name];
      if (!key_map) {
        callback(new Error('No key map named ' + key_map_name + ' found'));
        return;
      }
      key_map.put(key, value, callback);
    }
  };
  
  var sendCommand = function(command) {
    var line_command = JSON.stringify({command: command}) + "\n";
    connection.write(line_command, 'utf8');
  }
  
  var connect = function () {
    connection = net.createConnection(self.options.source_port, self.source);
  };
  connect();
  
  connection.on('connect', function() {
    sendCommand('sync');
    carrier.carry(connection, function(line) {
      var command;
      try {
        command = JSON.parse(line);
      } catch (excp) {
        callback(new Error('Error parsing replicated line: "' + line + '": ' + excp.message));
        return;
      }
      
      command_queue.push(command);
      notify();
    });
  });
  
  connection.on('close', function() {
    var retry_timeout = self.reconnect_timeout + Math.floor(Math.random() * self.reconnect_timeout) - Math.floor(self.reconnect_timeout / 2);
    setTimeout(connect, retry_timeout);
  });
  

};