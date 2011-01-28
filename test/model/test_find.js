var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util')
  , path   = require('path');

var DB_PATH = __dirname + '/../../tmp/db';

var USER = {name: 'Pedro', age: 30, sex: 'm', address: 'Estrada 1', email: 'pupup@gugu.co.uk', married: true};

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
  var alfred = require('../../lib/alfred');
  
  var timeout = setTimeout(function() {
    throw new Error('timeout');
  }, 5000);
  
  alfred.open(DB_PATH, function(err, db) {
    if (err) { next(err); return; }
    
    db.on('error', function(err) {
      next(err);
    });
    
    var User = db.define('User', {
      indexes: [
        {
          name: 'age',
          fn: function(user) {
            return user.age;
          }
        }
      ]
    });
    User.property('name');
    User.property('address');
    User.property('age', Number);
    User.property('married', 'boolean');
    User.property('email', 'string');
    
    var user = User.new(USER);
    user.save(function(errors) {
      assert.ok(errors === null, 'errors is not null');
      var left = 3;
      var done = function() {
        if (-- left == 0) {
          clearTimeout(timeout);
          db.close(next);
        }
      }
      User.find({age: {$lte: 30}}) (function(gotUser) {
        assert.ok(user.equal(gotUser), 'user ('+user.toString()+') and gotten user ('+gotUser.toString()+') are different');
        done();
      }).all(function(users) {
        assert.equal(1, users.length);
        assert.ok(user.equal(users[0]), 'user ('+user.toString()+') and gotten user ('+users[0].toString()+') are different');
        done();
      }).where({age: {$gt: 30}}).all(function(users) {
        assert.equal(0, users.length);
        done();
      });
    });

  });
};