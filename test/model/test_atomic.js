var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util');

var DB_PATH = __dirname + '/../../tmp/db';

var USER = {name: 'Pedro', age: 30, sex: 'm', address: 'Estrada 1', email: 'pupup@gugu.co.uk', married: true};

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
    User.property('address');
    User.property('age', Number);
    User.property('married', 'boolean');
    User.property('email', 'string');
    
    var user = User.new(USER);
    user.save(function(errors) {
      assert.ok(errors === null, 'errors is not null');
      user.atomic(function(gotUser) {
        assert.ok(user.equal(gotUser), 'user ('+user.toString()+') and gotten user ('+gotUser.toString()+') are different');
        gotUser.name = 'Miguel';
        return gotUser;
      }, function(errors) {
        assert.ok(!errors, 'errors is present: ' + util.inspect(errors));
        db.close(next);
      });
    });

  });
};