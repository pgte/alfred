var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util');

var DB_PATH = __dirname + '/../../tmp/db';

var USERS = [
    {name: 'Pedro', age: 35, sex: 'm'}
  , {name: 'John', age: 32, sex: 'm'}
  , {name: 'Bruno', age: 28, sex: 'm'}
  , {name: 'Sandra', age: 35, sex: 'f'}
  , {name: 'Patricia', age: 42, sex: 'f'}
  , {name: 'Joana', age: 29, sex: 'f'}
  , {name: 'Susana', age: 30, sex: 'f'}
];

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
    
    db.on('error', function(err) {
      next(err);
    });
    
    var User = db.define('User');
    User.property('name');
    User.property('age', Number);
    User.property('sex', 'string', {
      required: true,
      minimum: 1,
      maximum: 1
    });
    
    var left = USERS.length;
    var users = [];
    
    USERS.forEach(function(USER) {
      var user = User.instantiate(USER);
      user.save(function(errors) {
        if (errors) { next(new Error('validation errors: ' + util.inspect(errors))); }
        users.push(user);
        if (-- left === 0) {
          left = users.length;
          users.forEach(function(user) {
            User.get(user.id, function(gotUser) {
              assert.ok(user.equal(gotUser), 'user ('+user.toString()+') and gotten user ('+gotUser.toString()+') are different');
              if (-- left === 0) {
                clearTimeout(timeout);
                next();
              }
            });
          });
        }
      });
    });

  });
};