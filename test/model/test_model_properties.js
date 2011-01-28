var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util');

var DB_PATH = __dirname + '/../../tmp/db';

var USER = {name: 'Pedro', age: 35, sex: 'm'};
var UNKNOWN_PROP_NAME = 'unknownProperty';

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
    
    var user = User.new(USER);
    user[UNKNOWN_PROP_NAME] = 'abc';
    user.save(function(errors) {
      if (errors) { next(new Error('validation errors: ' + util.inspect(errors))); return; }
      User.get(user.id, function(gotUser) {
        assert.ok(user.equal(gotUser), 'user ('+user.toString()+') and gotten user ('+gotUser.toString()+') are different');
        assert.ok(gotUser[UNKNOWN_PROP_NAME] === undefined);
        clearTimeout(timeout);
        db.close(next);
      });
    });

  });
};