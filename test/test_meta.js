var assert     = require('assert');

module.exports.run = function(next) {
  var alfred = require('../lib/alfred');
  alfred.open('tmp/db', function(err, db) {
    if (err) {
      throw err;
    }
    db.attach_key_map('users', null, function(err, key_map) {
      if (err) {
        throw err;
      }
      db.users.put('abc', 'def', function(err) {
        if (err) {
          throw err;
        }
        db.users.get('abc', function(err, value) {
          assert.equal(value, 'def');
        });
        
        db.close(function(err) {
          if (err) {
            throw err;
          }
          
          // Reopen database and see if users key map is still there
          alfred.open('tmp/db', function(err, db) {
            if (err) {
              throw err;
            }
            assert.ok(!!db.users, 'db.users no longer exists');
            db.users.get('abc', function(err, value) {
              assert.equal(value, 'def');
              next();
            });
          });
        })
      })
    })
  });
};