var assert = require('assert')
  , fs     = require('fs');

var DB_PATH = __dirname + '/../tmp/db';

module.exports.setup = function(next) {
  fs.readdirSync(DB_PATH).forEach(function(dir) {
    fs.unlinkSync(DB_PATH + '/' + dir);
  });
  next();
};

module.exports.run = function(next) {
  var alfred = require('../lib/alfred');
  
  alfred.open(DB_PATH, function(err, db) {
    if (err) { next(err); return; }
    
    var done = function() {
      db.users.put('abc', 'def', function(err) {
        if (err) { next(err); return; }
        db.users.get('abc', function(err, value) {
          if (err) { next(err); return; }
          assert.equal(value, 'def');
        });

        db.close(function(err) {
          if (err) { next(err); return; }
          
          // Reopen database and see if users key map is still there
          alfred.open(DB_PATH, function(err, db) {
            if (err) { next(err); return; }
            assert.ok(!!db.users, 'db.users no longer exists');
            db.users.get('abc', function(err, value) {
              if (err) { next(err); return; }
              assert.equal(value, 'def');

              // Detach
              db.detach_key_map('users', function(err) {
                if (err) { next(err); return; }
                assert.ok(!!!db.users);

                db.close(function(err) {
                  if (err) { next(err); return; }
                  alfred.open(DB_PATH, function(err, db) {
                    if (err) { next(err); return; }
                    assert.ok(!!!db.users);
                    db.close(function(err) {
                      if (err) { next(err); return; }
                      next();
                    });
                  });
                });
              });
            });
          });
        });
      });
    };

    if (!db.users) {
      db.attach_key_map('users', null, function(err, key_map) {
        if (err) { next(err); return; }
        done();
      });
    } else {
      done();
    }
  });
};