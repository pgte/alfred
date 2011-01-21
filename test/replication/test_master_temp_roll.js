var assert        = require('assert')
  , fs            = require('fs')
  , net           = require('net')
  , child_process = require('child_process')
  , util          = require('util')
  , carrier       = require('carrier')
  , random_generator = require('../../tools/random_generator');
  
var DB_PATH = __dirname + '/../../tmp/db';

module.exports.setup = function(next) {
  (function removeFilesUnder(dir) {
    fs.readdirSync(dir).forEach(function(path) {
      var path = dir + '/' + path;
      var stat = fs.statSync(path);
      if (stat.isFile()) {
        fs.unlinkSync(path);
      } else {
        removeFilesUnder(path);
        fs.rmdirSync(path);
      }
    });
  })(DB_PATH);
  next();
};

var child, master;

process.on('uncaughtException', function(excp) {
  if (excp.message || excp.name) {
    if (excp.name) process.stdout.write(excp.name);
    if (excp.message) process.stdout.write(excp.message);
    if (excp.backtrace) process.stdout.write(excp.backtrace);
    if (excp.stack) process.stdout.write(excp.stack);
  } else {
    var util = require('util');
    process.stdout.write(util.inspect(excp));    
  }
  process.stdout.write("\n");
});


module.exports.run = function(next) {
  
  if (!process.env._TEST_MASTER) {
    // spawn master and mock slave

    var timeout = setTimeout(function() {
      assert.ok(false, 'timeout');
    }, 100000);
    
    var args = process.argv;
    var command = args.splice(0, 1)[0];
    var env = process.env;
    var exiting = false;
    
    env._TEST_MASTER = 'master';
    master = child_process.spawn(command, args, {env: env})
    
    var data_outer = function(data) {
      console.log(data.toString('utf8'));
    };
    master.stdout.on('data', data_outer);
    master.stderr.on('data', data_outer);
    master.on('exit', function() {
      master = undefined;
      assert.ok(exiting, 'master died');
    });

    env._TEST_MASTER = 'slave';
    child = child_process.spawn(command, args, {env: env})
    child.stdout.on('data', data_outer);
    child.stderr.on('data', data_outer);
    
    child.on('exit', function() {
      exiting = true;
      master.kill();
      clearTimeout(timeout);
      child = undefined;
      next();
    });
    child.stderr.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    
  } else {

    if (process.env._TEST_MASTER == 'master') {
      var alfred = require('../../lib/alfred');

      var timeout = setTimeout(function() {
        next(new Error('timeout'));
      }, 60000);
      
      var db_options = {
        replication_master: true,
        replication_max_file_size_kb: 10
      };

      alfred.open(DB_PATH, db_options, function(err, db) {
        if (err) { next(err); return; }
        setTimeout(function() {
          db.ensure_key_map_attached('store', null, function(err) {
            if (err) { next(err); return;}
            
            var put_count = 0;
            
            setTimeout(function() {
              var i = 0;
              var interval = setInterval(function() {
                var obj = random_generator.createRandomObject();
                obj.__index = i;
                db.store.put(random_generator.createRandomString(), obj, function(err) {
                  if (err) { next(err); return; }
                });
                i ++;
                if (i == 1000) {
                  clearInterval(interval);
                }
              }, 10);
            }, 3000);
          })
          
        }, 1000);
      });
      
    } else {
      // mock-slave
      
      var sendJSON = function(stream, data) {
        stream.write(JSON.stringify(data) + "\n");
      };
      
      var timeout = setTimeout(function() {
        next(new Error('timeout'));
        return;
      }, 100000);
      
      setTimeout(function() {
        
        var conn = net.createConnection(5293);
        
        conn.on('error', function(err) {
          next(err);
        });
        
        conn.on('connect', function() {
          sendJSON(conn, {command: 'sync'});
          
          var result_count = 0;
          
          carrier.carry(conn, function(line) {
            var obj = JSON.parse(line);
            if ('error' in obj) {
              next(new Error('Error from master: ' + obj.error));
              return;
            }
            if (obj.m != 'meta') {
              assert.equal(obj.m, 'store');
              assert.equal(result_count, obj.v.__index);
              result_count ++;
              if (result_count == 1000) {
                clearTimeout(timeout);
                setTimeout(function() {
                  next();
                }, 500)
              }
            }
          });

        });
        

      }, 2000);
    }

  }

};

module.exports.teardown = function() {
  if (master) {
    master.kill();
  }
  master = undefined;
  if (child) {
    child.kill();
  }
  child = undefined;
};