var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util')
  , path   = require('path');

var DB_PATH = __dirname + '/../../tmp/db';

var WRONG_USER = {name: 'Pedro Gonçalves Teixeira', sex: 'k', address: 'a', married: 1, email: 'pupup@gugu', role: 'gugu'};
var EXPECTED_ERRORS = [
  { attribute: 'maxLength',
    property: 'name',
    expected: 10,
    actual: 24,
    message: 'is too long' },
  { attribute: 'minLength',
    property: 'address',
    expected: 3,
    actual: 1,
    message: 'is too short' },
  { attribute: 'optional',
    property: 'age',
    expected: undefined,
    actual: true,
    message: 'is not optional' },
  { attribute: 'type',
    property: 'age',
    expected: 'number',
    actual: 'undefined',
    message: 'is of the wrong type' },
  { attribute: 'enum',
    property: 'sex',
    expected: [ 'f', 'm' ],
    actual: 'k',
    message: 'is invalid' },
  { attribute: 'type',
    property: 'married',
    expected: 'boolean',
    actual: 'number',
    message: 'is of the wrong type' },
  { attribute: 'pattern',
    property: 'email',
    expected: /^([a-zA-Z0-9_\.-])+@(([a-zA-Z0-9-])+\.)+[a-zA-Z0-9]{2,4}/,
    actual: 'pupup@gugu',
    message: 'IINNVVAALLIID' },
  { attribute: 'validateWith',
    property: 'role',
    expected: 'Function',
    actual: 'gugu',
    message: 'is invalid'
  }
];
    
var RIGHT_USER = {name: 'Pedro', age: 30, sex: 'm', address: 'Estrada 1', email: 'pupup@gugu.co.uk', married: true, role: 'admin'};

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
    User.property('name', 'string', {
      minLength: 3,
      maxLength: 10
    });
    User.property('address', 'string', {
      minLength: 3,
      maxLength: 10
    });
    User.property('age', Number, { required: true });
    User.property('sex', 'string', {
      enum: ['f', 'm'],
      required: true,
    });
    User.property('married', 'boolean');
    User.property('email', 'string', {
      pattern: /^([a-zA-Z0-9_\.-])+@(([a-zA-Z0-9-])+\.)+[a-zA-Z0-9]{2,4}/,
      messages: {pattern: 'IINNVVAALLIID'}
    });
    User.property('role', 'string', {
      validateWith: function(role) {
        return role == 'admin';
      }
    });
    
    var user = User.new(WRONG_USER);
    user.save(function(errors) {
      assert.ok(errors !== null, 'errors is null');
      assert.deepEqual(errors, EXPECTED_ERRORS);
      var rightUser = User.new(RIGHT_USER);
      rightUser.save(function(errors) {
        assert.ok(errors === null, util.inspect(errors) + " is not null");
        db.close(next);
      });
    });

  });
};