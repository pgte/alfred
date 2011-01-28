var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util')
  , path   = require('path');

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
      var user = User.new(USER);
      user.save(function(errors) {
        if (errors) { next(new Error('validation errors: ' + util.inspect(errors))); return; }
        users.push(user);
        if (-- left === 0) {
          user.destroy(function() {
            User.get(user.id, function(gotUser) {
              assert.ok(gotUser === null);
              db.close(next);
            });
          });
        }
      });
    });

  });
};