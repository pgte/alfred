var assert        = require('assert')
  , fs            = require('fs')
  , net           = require('net')
  , child_process = require('child_process')
  , util          = require('util');
  
var MASTER_DB_PATH = __dirname + '/../../tmp/db';
var SLAVE_DB_PATH = __dirname + '/../../tmp/db2';

var USERS = {
    1: {name: 'Pedro', age: 35, sex: 'm'}
  , 2: {name: 'John', age: 32, sex: 'm'}
  , 3: {name: 'Bruno', age: 28, sex: 'm'}
  , 4: {name: 'Sandra', age: 35, sex: 'f'}
  , 5: {name: 'Patricia', age: 42, sex: 'f'}
  , 6: {name: 'Joana', age: 29, sex: 'f'}
  , 7: {name: 'Susana', age: 30, sex: 'f'}
};

var USER_COUNT = 7;

module.exports.setup = function() {
  fs.readdirSync(MASTER_DB_PATH).forEach(function(dir) {
    fs.unlinkSync(MASTER_DB_PATH + '/' + dir);
  });
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
    }, 10000);
    
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
      master = undefined;
      assert.ok(exiting, 'master died');
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
      exiting = true;
      master.kill();
      child = undefined;
      
      var alfred = require('../../lib/alfred');
      
      alfred.open(SLAVE_DB_PATH, function(err, slave_db) {
        if (err) { next(err); return; }
        assert.ok(!!slave_db.users, 'no users database on slave');
        assert.ok(!!slave_db.users.indexes.age, 'no users.age index on slave');
        assert.ok(!!slave_db.users.indexes.sex, 'no users.sex index on slave');
        
        var got_users = 0;
        
        for(id in USERS) {
          if (USERS.hasOwnProperty(id)) {
            (function(id) {
              var user = USERS[id];
              slave_db.users.get(id, function(err, value) {
                if (err) { next(err); return; }
                assert.ok(!!value);
                assert.equal(value.name, user.name);
                assert.equal(value.age, user.age);
                assert.equal(value.rndm, id);
                
                got_users ++;
                if (got_users == USER_COUNT) {
                  clearTimeout(timeout);
                  next();
                }
              });
            })(id);
          }
        }
      });
      
    });
    child.stderr.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    
  } else {
    
    // Master

    if (process.env._TEST_MASTER == 'master') {
      var alfred = require('../../lib/alfred');

      var timeout = setTimeout(function() {
        next(new Error('timeout'));
      }, 10000);
      
      alfred.open(MASTER_DB_PATH, {replication_master: true}, function(err, db) {
        if (err) { next(err); return; }
        setTimeout(function() {
          db.ensure_key_map_attached('users', null, function(err) {
            if (err) { next(err); return;}

            var age_transform_function = function(user) {
              return user.age;
            };

            var sex_transform_function = function(user) {
              return user.sex;
            };
            
            db.users.ensureIndex('sex', {ordered: true}, sex_transform_function, function(err) {
              if (err) { next(err); return; }
              db.users.ensureIndex('age', {ordered: true}, age_transform_function, function(err) {
                if (err) { next(err); return; }

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
                          
                          setTimeout(function() {
                            var more_users_count = 0;
                            for(var id in USERS) {
                              if (USERS.hasOwnProperty(id)) {
                                (function(id) {
                                  var user = USERS[id];
                                  user.rndm = id;
                                  db.users.put(id, user, function(err) {
                                    if (err) { next(err); return; }
                                    more_users_count ++;
                                    if (more_users_count == USER_COUNT) {
                                    }
                                  });
                                })(id);
                              }
                            }
                          }, 2000);
                        }
                      });
                    })(id);
                  }
                }
              });
            });
          })
          
        }, 1000);
      });
      
    } else {
      
      // SLAVE
      
      setTimeout(function() {
        next();
      }, 5000);
      
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