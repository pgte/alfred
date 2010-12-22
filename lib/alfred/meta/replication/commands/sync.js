var temp_file = require('../../../files/temp_file');
var StringDecoder = require('string_decoder').StringDecoder;

var BUFFER_SIZE = 64 * 1024;

module.exports = function(master, stream) {
  var listen_handle,
      key_maps,
      current_file,
      waiting_files = [];
  var ending = false;
  var notifying = false;
  
  var ender = function() {
    ending = true;
    if (listen_handle) {
      master.database.stopListening(listen_handle);
      listen_handle = undefined;
    }
    waiting_files.forEach(function(waiting_file) {
      waiting_file.end(function(err) {
        if (err) {
          master.error(err);
          return;
        }
        waiting_file.destroy(function(err) {
          if (err) {
            master.error(err);
          }
        });
      });
    });
  }
  
  stream.on('end', ender);
  stream.on('close', ender);
  
  // Stream existing data on all key maps into a backlog file
  temp_file.open(function(err, backlog_file) {
    
    if (err) {
      master.error(stream, err);
      return;
    }
    
    backlog_file._written_bytes = 0;
    var send = function(data) {
      backlog_file.write(JSON.stringify(data) + "\n", function(err, pos, length) {
        if (err) {
          master.error(stream, err);
          return;
        }
        backlog_file._written_bytes += (length + 1);
        process.nextTick(function() {
          notify(); // notify sending stream so data gets to the client
        });
        
      });
    };
    
    key_maps = master.database.key_map_names.map(function(key_map_name) {
      return {name : key_map_name, key_map: master.database[key_map_name]};
    });

    var pipe_collection_read_into_file = function(key_map_name, callback) {
      return function(err, record) {
        if (err) {
          master.error(stream, err);
          return;
        }
        if (record != null) {
          send({m: key_map_name, k: record.k, v: record.v});
        } else {
          callback();
        }
      };
    }
    
    master.database.meta.collection.read(pipe_collection_read_into_file('meta', function() {
      
      backlog_file._finished = true;
      process.nextTick(function() {
        notify(); // notify sending stream so data gets to the client
      });

      // done with meta.
      // Now we can pipe all other key_maps
      backlog_file._sync_finished_writing_file_count = 0;
      key_maps.forEach(function(key_map) {
        key_map.key_map.collection.read(pipe_collection_read_into_file(key_map.name, function() {
          backlog_file._sync_finished_writing_file_count ++;
          if (backlog_file._sync_finished_writing_file_count == key_maps.length) {
            // We are done with backlog
            // Signal it
            delete backlog_file._sync_finished_writing_file_count;
            backlog_file._sync_finished_writing = true;
          }
        }), true);
      });
      
    }), true);
    

    waiting_files[0] = backlog_file;

  });
  
  // Stream new records into a running file
  temp_file.open(function(err, running_file) {
    if (err) {
      master.error(stream, err);
      return;
    }
    var send = function(data) {
      running_file.write(JSON.stringify(data) + "\n", function(err, pos, length) {
        if (err) {
          master.error(stream, err);
          return;
        }
        process.nextTick(function() {
          notify(); // notify sending stream so data gets to the client
        });
      });
    };
    
    listen_handle = master.database.listenOnAllRegisteredKeyMaps(function(key_map_name, key, value) {
      send({m: key_map_name, k: key, v: value});
    });
    
    waiting_files[1] = running_file;
  });
  
  // Stream these files downstream to the client
  
  var string_decoder = new StringDecoder('utf8');
  
  var pending_notifies = 0;
  
  var notify = function() {
    
    if (notifying) {
      pending_notifies ++;
      return;
    }
    
    if (waiting_files.length < 1) {
      return;
    }

    if (pending_notifies > 0) {
      pending_notifies --;
    }

    sending_file = waiting_files[0];
    
    if (!sending_file._sent_bytes) {
      sending_file._sent_bytes = 0;
    }
    
    var pos = sending_file._sent_bytes;
    
    var length = BUFFER_SIZE;
    
    notifying = true;
    
    sending_file.try_read(pos, length, function(err, buffer, bytesRead) {
      notifying = false;
      
      if (err) {
        master.error(stream, err);
        return;
      }
      
      if (bytesRead > 0) {
        sending_file._sent_bytes += bytesRead;
        stream.write(string_decoder.write(buffer), 'utf8');
        process.nextTick(function() {
          notify();
        });
      } else {
        if (sending_file._finished) {
          if (waiting_files.length > 1) {
            waiting_files.splice(0, 1);
          }
          process.nextTick(function() {
            notify();
          });
        } else {
          if (pending_notifies > 0) {
            process.nextTick(function() {
              notify();
            });
          }
        }
      }
    });
  };

};