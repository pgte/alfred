var assert = require('assert')
  , fs     = require('fs')
  , path   = require('path')
  , spawn  = require('child_process').spawn;

var DB_PATH = __dirname + '/../tmp/db';

var USERS = {
    1: {name: 'Pedro', age: 35}
  , 2: {name: 'John', age: 32}
  , 3: {name: 'Bruno', age: 28}
  , 4: {name: 'Sandra', age: 35}
};

var USER_COUNT = 4;

module.exports.setup = function(next) {
  (function removeFilesUnder(dir) {
    if (path.existsSync(dir)) {
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
    }
  })(DB_PATH);
  next();
};

module.exports.run = function(next) {
  var alfred = require('../lib/alfred');
  
  if (!process.env._ALFRED_TEST_CHILD) {
    var timeout = setTimeout(function() {
      assert.ok(false, 'timeout');
    }, 10000);
    
    var data_outer = function(data) {
      console.log("child: " + data.toString());
    };
    
    var args = process.argv;
    var command = args.splice(0, 1)[0];
    var env = process.env;
    
    env._ALFRED_TEST_CHILD = true;
    child = spawn(command, args, {env: env})
    child.stdout.on('data', data_outer);
    child.stderr.on('data', data_outer);
    
    child.on('exit', function() {
      alfred.open(DB_PATH, function(err, db) {
        if (err) { next(err); return; }
        assert.ok(!!db.users, 'db.users does not exist');
        var users_found = 0;
        db.users.scan(function(err, key, value) {
          if (key === null) {
            assert.equal(USER_COUNT, users_found, "expected to find " + USER_COUNT + ' users and found ' + users_found);
            clearTimeout(timeout);
            db.close(next);
            return;
          }
          users_found ++;
        }, true);
      });
    });

  } else {
    var timeout = setTimeout(function() {
      throw new Error('timeout');
    }, 5000);

    alfred.open(DB_PATH, function(err, db) {
      if (err) { next(err); return; }
      db.ensure_key_map_attached('users', null, function(err) {
        if (err) { next(err); return; }

        var transform_function = function(user) {
          return user.age;
        };

        var users_in = 0;
        for (var id in USERS) {
          if (USERS.hasOwnProperty(id)) {
            (function(id) {
              var user = USERS[id];
              db.users.put(id, user, function(err) {
                if (err) { next(err); return; }
                users_in ++;
                if (users_in == USER_COUNT) {
                  // all users done
                  next();
                }
              });
            })(id);
          }
        }
      })
    });
  }

};