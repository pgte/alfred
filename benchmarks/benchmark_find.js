var fs     = require('fs'),
    assert = require('assert'),
    Alfred = require('../lib/alfred'),
    RandomGenerator = require('../tools/random_generator');

var DB_PATH = __dirname + '/../tmp/db';
var DOC_COUNT = 100000;
var FIND_COUNT = 1000;

module.exports.setup = function() {
  fs.readdirSync(DB_PATH).forEach(function(dir) {
    fs.unlinkSync(DB_PATH + '/' + dir);
  });
};

module.exports.run = function(benchmark, next) {
  Alfred.open(DB_PATH, function(err, db) {
    if (err) { throw err; }
    db.ensure('users', function(err) {
      var user
        , ageIndexer;
      if (err) { throw err; }
      
      ageIndexer = function(user) {
        return user.age;
      };
      
      db.users.addIndex('age', {ordered: true}, ageIndexer, function(err) {
        var aIndexer;
        if (err) { throw err; }
        
        aIndexer = function(user) {
          return user.a;
        };
        
        db.users.addIndex('a', {ordered: true}, aIndexer, function(err) {
          var added = 0;
          if (err) { throw err; }

          benchmark.start('populating keymap that has 2 attached indexes', DOC_COUNT);
          
          for(var i = 1; i <= DOC_COUNT; i++) {
            user = RandomGenerator.createRandomObject();
            user.age = Math.floor(Math.random() * 100);
            db.users.put(i, user, function(err) {
              if (err) { throw err; }
              if (++ added == DOC_COUNT) {
                var found = 0;
                benchmark.end();
                
                benchmark.start('finder with no get', FIND_COUNT);
                
                for (var ii = 0; ii < FIND_COUNT; ii++) {
                  db.users.find({age: {$eq: RandomGenerator.createRandomString(90)}}).bulk(function(err, users) {
                    if (err) { throw err; }
                    found ++;
                    assert.equal(0, users.length);
                    if (found == FIND_COUNT) {
                      benchmark.end();
                      (function() {
                        var found = 0;
                        benchmark.start('find $eq operator', FIND_COUNT);
                        for (var j = 0; j < FIND_COUNT; j++) {
                          db.users.find({age: {$eq : Math.floor(Math.random() * 100)}}).bulk(function(err) {
                            if (err) { throw err; }

                            if (++found == FIND_COUNT) {

                              found = 0;

                              benchmark.end();

                              benchmark.start('find $range operator', FIND_COUNT);
                              for (var k = 0; k < FIND_COUNT; k++) {
                                var new_age = Math.floor(Math.random() * 100)
                                db.users.find({age: {$range : [new_age, new_age + 1]}}).bulk(function(err) {
                                  if (err) { throw err; }
                                  if (++found == FIND_COUNT) {
                                    benchmark.end();
                                    db.close(function(err) {
                                      if (err) { throw err; }
                                      next();
                                    });
                                  }
                                });
                              }
                            }
                          });
                        }
                      })();
                    }
                  });
                }

              }
            });
          }
        });
        
      });
      
    });
  });
};