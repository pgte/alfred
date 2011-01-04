var assert = require('assert')
  , fs     = require('fs');

var DB_PATH = __dirname + '/../../tmp/db';

var USERS = {
    1: {name: 'Pedro', age: 35}
  , 2: {name: 'John', age: 32}
  , 3: {name: 'Bruno', age: 28}
  , 4: {name: 'Sandra', age: 35}
};

var USER_COUNT = 4;

module.exports.setup = function(next) {
  fs.readdirSync(DB_PATH).forEach(function(dir) {
    fs.unlinkSync(DB_PATH + '/' + dir);
  });
  next();
};

module.exports.run = function(next) {
  var alfred = require('../../lib/alfred');
  
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
      
      db.users.ensureIndex('age', {ordered: true}, transform_function, function(err) {
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
                  
                  var users_found = 0;
                  
                  db.users.find({'age' : {$eq: 35}}) (function(err, key, value) {
                    if (err) { next(err); return; }
                    assert.deepEqual(value, USERS[key]);
                    assert.ok(value.age == 35, 'age is not equal to 35 for found user with key ' + key);
                    users_found ++;
                    assert.ok(users_found <= 2);
                    if (users_found == 2) {
                      clearTimeout(timeout);
                      db.close(function(err) {
                        if (err) { next(err); return; }
                        next();
                      })
                    }
                  });
                }
              });
            })(id);
          }
        }
      });
    })
  });
};