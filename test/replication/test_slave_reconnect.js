var assert        = require('assert')
  , fs            = require('fs')
  , net           = require('net')
  , child_process = require('child_process')
  , util          = require('util')
  , carrier       = require('carrier');
  
var SLAVE_DB_PATH = __dirname + '/../../tmp/db2';

module.exports.setup = function() {
  fs.readdirSync(SLAVE_DB_PATH).forEach(function(dir) {
    fs.unlinkSync(SLAVE_DB_PATH + '/' + dir);
  });
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
    }, 30000);
    
    var args = process.argv;
    var command = args.splice(0, 1)[0];
    var env = process.env;
    var exiting = false;
    
    env._TEST_MASTER = 'master';
    master = child_process.spawn(command, args, {env: env});
    
    var data_outer = function(prefix) {
      return function(data) {
        console.log(prefix + ":  >>>>:\n" + data.toString('utf8') + "\n  <<<<");
      };
    };
    
    master.stdout.on('data', data_outer('master'));
    master.stderr.on('data', data_outer('master'));
    master.on('exit', function() {
      exiting = true;
      child.kill();
      child = undefined;
      
      clearTimeout(timeout);
      next();
    });

    var new_env = {};
    for(var envvar in env) {
      new_env[envvar] = process.env[envvar];
    }
    new_env._TEST_MASTER = 'slave';
    child = child_process.spawn(command, args, {env: new_env})
    child.stdout.on('data', data_outer('slave'));
    child.stderr.on('data', data_outer('slave'));
    
    child.on('exit', function() {
      master = undefined;
      assert.ok(exiting, 'slave died');
      
    });
    child.stderr.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    
  } else {

    if (process.env._TEST_MASTER == 'master') {
      
      // Master

      var timeout = setTimeout(function() {
        assert.ok(false, 'timeout');
      }, 30000);
      
      var max_connects = 10;
      var connects = 0;

      net.createServer(function(stream) {
        connects ++;
        if (max_connects == connects) {
          clearTimeout(timeout);
          next();
          return;
        }
        carrier.carry(stream, function(line) {
          assert.equal(line, '{"command":"sync"}');
          
          stream.end();
          
        });
      }).listen(5293); // default port

    } else {

      // SLAVE

      setTimeout(function() {
        next();
      }, 30000);
      
      var alfred = require('../../lib/alfred');

      alfred.open(SLAVE_DB_PATH, function(err, db) {
        if (err) { next(err); return; }
        
        setTimeout(function() {
          db.replicate_from('localhost', {}, function(err) {
            next(err);
            throw err;
          });
        }, 2000);
        
      });
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