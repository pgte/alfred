var assert = require('assert')
  , fs     = require('fs');

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
                  
                    var users_1_found = 0;
                    var users_2_found = 0;
                    var users_3_found = 0;
                    var users_4 = false
                    
                    var end_test = function() {
                      if (users_1_found == 2 && users_2_found == 4 && users_3_found == 6 && users_4) {
                        db.close(function(err) {
                          if (err) { next(err); return; }
                          clearTimeout(timeout);
                          next();
                        })
                      }
                    };
                  
                    db.users.find({'age' : {$gt: 29, $lt: 42, $lte: 35}, 'sex': {$eq: 'f'}}) (function(err, key, value) {
                      if (err) { next(err); return; }
                      assert.deepEqual(value, USERS[key]);
                      assert.ok(value.age > 29 && value.age <= 35, 'age is not equal to > 29 and < 35 for found user with key ' + key);
                      assert.ok(value.sex =='f', 'sex != \'f\' for found user with key ' + key);
                      users_1_found ++;
                      assert.ok(users_1_found <= 2, 'already found ' + users_1_found + ' users');
                      end_test();
                    })
                    .reset()
                    .where({'age' : {$gt: 29, $lt: 42}})
                      (function(err, key, value) {
                        if (err) { next(err); return; }
                        assert.deepEqual(value, USERS[key]);
                        assert.ok(value.age > 29 && value.age < 42, 'age is not equal to > 29 and < 35 for found user with key ' + key);
                        users_2_found ++;
                        assert.ok(users_2_found <= 4, 'already found ' + users_2_found + ' users');
                        end_test();
                      })
                    .reset()
                    .where({'age' : {$gt: 29, $lt: 35}})
                    .or({'sex': {$eq: 'f'}})
                    (function(err, key, value) {
                      if (err) { next(err); return; }
                      assert.deepEqual(value, USERS[key]);
                      assert.ok((value.age > 29 && value.age < 35) || value.sex == 'f', '(age is not equal to > 29 and < 35) or sex == \'f\' for found user with key ' + key);
                      users_3_found ++;
                      assert.ok(users_3_found <= 6, 'already found ' + users_3_found + ' users');
                      end_test();
                    })
                    .bulk(function(err, records) {
                      if (err) { next(err); return; }
                      assert.equal(6, records.length);
                      records.forEach(function(record) {
                        assert.deepEqual(record.value, USERS[record.key]);
                      });
                      users_4 = true;
                      end_test();
                    });
                  }
                });
              })(id);
            }
          }
        });
      });
    })
  });
};