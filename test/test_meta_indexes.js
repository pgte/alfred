var assert     = require('assert');

var DB_PATH = __dirname + '/../tmp/db';

module.exports.run = function(next) {
  var alfred = require('../lib/alfred');
  alfred.open(DB_PATH, function(err, db) {
    if (err) { throw err; }
    
    var done = function(err) {
      if (err) { throw err; }
      var transform_function = function(record) {
        return record;
      };
      assert.ok(!!db.users);
      
      var done_with_index = function() {
        assert.ok(!!db.users.idx);
        
        db.users.put('abc', 'def', function(err) {
          if (err) { throw err; }
          var match_count = 0;
          db.users.idx.indexMatch('def', function(err, value) {
            if (err) { throw err; }
            match_count ++;
            assert.equal(1, match_count, 'more than one match');
            if (err) { throw err; }
            db.drop_index('users', 'idx', function(err) {
              
              assert.ok(!!!db.users.idx, 'index not removed');
              
              db.users.addIndex('idx', {ordered: true}, transform_function, function(err) {
                assert.ok(!!db.users.idx);
                db.close(function(err) {
                  alfred.open(DB_PATH, function(err, db) {
                    if (err) { throw err; }
                    db.users.idx.indexMatch('def', function(err, value) {
                      if (err) { throw err; }
                      assert.ok(!!db.users, 'db.users is no longer here after DB restart');
                      assert.ok(!!db.users.idx, 'db.users.idx is no longer here after DB restart');
                      next();
                    });
                  });
                });
              });
            })
          });
        });
        
      };
      
      if (!db.users.idx) {
        db.users.addIndex('idx', {ordered : true}, transform_function, function(err) {
          if (err) { throw err; }
          done_with_index();
        });
      } else {
        done_with_index();
      }
    }
    
    if (!db.users) {
      db.attach_key_map('users', done);
    } else {
      done(null);
    }
  });
};