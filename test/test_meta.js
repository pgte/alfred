var assert     = require('assert');

var DB_PATH = __dirname + '/../tmp/db';

module.exports.run = function(next) {
  var alfred = require('../lib/alfred');
  
  alfred.open(DB_PATH, function(err, db) {
    if (err) { throw err; }
    if (!db.users) {
      db.attach_key_map('users', null, function(err, key_map) {
        if (err) { throw err; }
        done();
      });
    } else {
      done();
    }
    
    var done = function() {
      db.users.put('abc', 'def', function(err) {
        if (err) { throw err; }
        db.users.get('abc', function(err, value) {
          if (err) { throw err; }
          assert.equal(value, 'def');
        });

        db.close(function(err) {
          if (err) { throw err; }

          // Reopen database and see if users key map is still there
          alfred.open(DB_PATH, function(err, db) {
            if (err) { throw err; }
            assert.ok(!!db.users, 'db.users no longer exists');
            db.users.get('abc', function(err, value) {
              assert.equal(value, 'def');

              // Detach
              db.detach_key_map('users', function(err) {
                if (err) { throw err; }
                assert.ok(!!!db.users);

                db.close(function(err) {
                  if (err) { throw err; }
                  alfred.open(DB_PATH, function(err, db) {
                    if (err) { throw err; }

                    assert.ok(!!!db.users);

                    next();
                  });
                });
              });
            });
          });
        });
      });
    };
    
  });


};