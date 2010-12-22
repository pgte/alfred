var assert        = require('assert')
  , fs            = require('fs')
  , net           = require('net')
  , child_process = require('child_process')
  , util          = require('util')
  , carrier       = require('carrier');
  
var DB_PATH = __dirname + '/../../tmp/db';

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
  fs.readdirSync(DB_PATH).forEach(function(dir) {
    fs.unlinkSync(DB_PATH + '/' + dir);
  });
};

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
    var master = child_process.spawn(command, args, {env: env})
    master.stdout.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    master.on('exit', function() {
      assert.ok(exiting, 'master died');
    });
    master.stderr.on('data', function(data) {
      console.log(data.toString('utf8'));
    });

    env._TEST_MASTER = 'slave';
    var child = child_process.spawn(command, args, {env: env})
    child.stdout.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    
    child.on('exit', function() {
      exiting = true;
      master.kill();
      clearTimeout(timeout);
      next();
    });
    child.stderr.on('data', function(data) {
      console.log(data.toString('utf8'));
    });
    
  } else {

    if (process.env._TEST_MASTER == 'master') {
      var alfred = require('../../lib/alfred');

      var timeout = setTimeout(function() {
        throw new Error('timeout');
      }, 10000);

      alfred.open(DB_PATH, {replication_master: true}, function(err, db) {
        if (err) { throw err; }
        setTimeout(function() {
          db.ensure_key_map_attached('users', null, function(err) {
            if (err) { throw err; }

            var age_transform_function = function(user) {
              return user.age;
            };

            var sex_transform_function = function(user) {
              return user.sex;
            };

            db.users.ensureIndex('sex', {ordered: true}, sex_transform_function, function(err) {
              db.users.ensureIndex('age', {ordered: true}, age_transform_function, function(err) {
                if (err) { throw err; }

                var users_in = 0;
                for (var id in USERS) {
                  if (USERS.hasOwnProperty(id)) {
                    (function(id) {
                      var user = USERS[id];
                      db.users.put(id, user, function(err) {
                        if (err) { throw err; }
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
                                    if (err) { throw err; }
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
      // mock-slave
      
      var expected_objects = [
      { m: 'meta',
        k: 'key_maps',
        v: 
         { users: 
            { command: 'attach_key_map',
              arguments: 
               [ 'users',
                 { type: 'cached_key_map', compact_interval: 3600000 } ] } } }
      , { m: 'meta',
        k: 'indexes',
        v: 
         { users: 
            { sex: 
               { command: 'add_index',
                 arguments: 
                  [ 'users',
                    'sex',
                    { ordered: true },
                    'function (user) {\n              return user.sex;\n            }' ] } } } }
      , { m: 'meta',
        k: 'indexes',
        v: 
         { users: 
            { sex: 
               { command: 'add_index',
                 arguments: 
                  [ 'users',
                    'sex',
                    { ordered: true },
                    'function (user) {\n              return user.sex;\n            }' ] },
              age: 
               { command: 'add_index',
                 arguments: 
                  [ 'users',
                    'age',
                    { ordered: true },
                    'function (user) {\n              return user.age;\n            }' ] } } } }
      ,{ m: 'users',
        k: '1',
        v: { name: 'Pedro', age: 35, sex: 'm' } }
      ,{ m: 'users',
        k: '2',
        v: { name: 'John', age: 32, sex: 'm' } }
      ,{ m: 'users',
        k: '3',
        v: { name: 'Bruno', age: 28, sex: 'm' } }
      ,{ m: 'users',
        k: '4',
        v: { name: 'Sandra', age: 35, sex: 'f' } }
      , { m: 'users',
        k: '5',
        v: { name: 'Patricia', age: 42, sex: 'f' } }
      , { m: 'users',
        k: '6',
        v: { name: 'Joana', age: 29, sex: 'f' } }
      , { m: 'users',
        k: '7',
        v: { name: 'Susana', age: 30, sex: 'f' } }
      , { m: 'users',
        k: '1',
        v: { name: 'Pedro', age: 35, sex: 'm', rndm: '1' } }
      , { m: 'users',
        k: '2',
        v: { name: 'John', age: 32, sex: 'm', rndm: '2' } }
      , { m: 'users',
        k: '3',
        v: { name: 'Bruno', age: 28, sex: 'm', rndm: '3' } }
      , { m: 'users',
        k: '4',
        v: { name: 'Sandra', age: 35, sex: 'f', rndm: '4' } }
      , { m: 'users',
        k: '5',
        v: { name: 'Patricia', age: 42, sex: 'f', rndm: '5' } }
      , { m: 'users',
        k: '6',
        v: { name: 'Joana', age: 29, sex: 'f', rndm: '6' } }
      , { m: 'users',
        k: '7',
        v: { name: 'Susana', age: 30, sex: 'f', rndm: '7' } }
      ];
      
      var records = [];
      
      var sendJSON = function(stream, data) {
        stream.write(JSON.stringify(data) + "\n");
      };
      
      var timeout = setTimeout(function() {
        throw new Error('timeout');
      }, 10000);
      
      setTimeout(function() {
        var conn = net.createConnection(5293);
        
        conn.on('error', function(err) {
          throw err;
        });
        
        conn.on('connect', function() {
          sendJSON(conn, {command: 'sync'});
          
          var result_count = 0;
          
          carrier.carry(conn, function(line) {
            var obj = JSON.parse(line);
            records.push(obj)
            result_count ++;
            if (result_count == expected_objects.length) {
              assert.deepEqual(expected_objects, records);
              clearTimeout(timeout);
              next();
            }
          });

        });
        

      }, 2000);
    }

  }

};