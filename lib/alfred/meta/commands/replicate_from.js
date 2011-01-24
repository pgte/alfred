var options_merger = require('../../util/options_merger');
var net     = require('net'),
    carrier = require('carrier'),
    util    = require('util');

var default_options = {
  source_port: 5293,
  reconnect_count: 20,
  reconnect_timeout: 1000
};

var ReplicateFromCommand = function(source, options) {
  this.source = source;
  this.options = options_merger.merge(default_options, options);
};

var create = function(source, options) {
  return new ReplicateFromCommand(source, options);
}

module.exports.register = function(meta) {
  meta.registerCommand(['replicate_from', 'replicateFrom'], create);
};

ReplicateFromCommand.prototype.do = function(meta, callback) {
  var self = this;
  var connection;
  var reconnect_tries;
  var last_run_log_pos = 0;
  var logger_id;
  var reconnecting = false;
  
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
      do_command(command, function(err) {
        if (err) {
          callback(new Error('Error doing replication slave command \'' + util.inspect(command) + '\': ' + util.inspect(err)));
          return;
        }
        last_run_log_pos = command.__log_pos;
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
  };
  
  (function connect() {
    connection = net.createConnection(self.options.source_port, self.source);
    connection.on('connect', function() {
      reconnecting = false;
      sendCommand('sync', {from: last_run_log_pos});
      carrier.carry(connection, function(line) {
        var command;
        try {
          command = JSON.parse(line);
        } catch (excp) {
          callback(new Error('Error parsing replicated line: "' + line + '": ' + excp.message));
          return;
        }
        
        if (!logger_id) {
          logger_id = command.__logger_id;
        } else {
          if (logger_id != command.__logger_id) {
            if (!reconnecting) {
              reconnecting = true;
              logger_id = undefined;
              last_run_log_pos = 0;
              connection.end();
              reconnecting = true;
              process.setTimeout(connect, 1000);
            }
            return;
          }
        }
        
        command_queue.push(command);
        notify();

      });
      connection.on('close', function() {
        var retry_timeout = self.options.reconnect_timeout + Math.floor(Math.random() * self.options.reconnect_timeout) - Math.floor(self.options.reconnect_timeout / 2);
        setTimeout(connect, retry_timeout);
      });

    });

  })();
  

};