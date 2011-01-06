var assert        = require('assert')
  , child_process = require('child_process');
    
var DB_PATH = __dirname + '/../tmp/db';

var child;

module.exports.run = function(next) {
  
  if (!process.env._TEST_CHILD) {
    // spawn master and mock slave

    var timeout = setTimeout(function() {
      assert.ok(false, 'timeout');
    }, 10000);
    
    var args = process.argv;
    var command = args.splice(0, 1)[0];
    var env = process.env;
    var exiting = false;
    env._TEST_CHILD = true;
    var exiting = false;
    
    child = child_process.spawn(command, args)
    
    var data_outer = function(data) {
      console.log(data.toString('utf8'));
    };
    child.stdout.on('data', data_outer);
    child.stderr.on('data', data_outer);
    child.on('exit', function() {
      clearTimeout(timeout);
      next();
    });
    
  } else {

    var alfred = require('../lib/alfred');

    var timeout = setTimeout(function() {
      next(new Error('timeout'));
    }, 10000);

    alfred.open(DB_PATH, function(err, db) {
      if (err) { next(err); return; }
      db.ensureKeyMapAttached('users', null, function(err) {
        if (err) { next(err); return;}
        clearTimeout(timeout);
        db.close(function(err) {
          next(err, true);
        });

      });
    });

  }

};

module.exports.teardown = function() {
  if (child) {
    child.kill();
  }
  child = undefined;
};