// Test doc.inspect()

var assert = require('assert')
  , fs     = require('fs')
  , util   = require('util')
  , path   = require('path');

var DB_PATH = __dirname + '/../../tmp/db';

var USER = {name: 'Pedro', age: 35, sex: 'm'};
var EXPECTED_INSPECT = "{ name: 'Pedro', age: 35, sex: 'm', id: '<<<ID>>>' }";

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
    
    var user = User.new(USER);
    user.save(function(errors) {
      if (errors) { next(new Error('validation errors: ' + util.inspect(errors))); return; }
      User.get(user.id, function(gotUser) {
        assert.ok(user.equal(gotUser), 'user ('+user.toString()+') and gotten user ('+gotUser.toString()+') are different');
        EXPECTED_INSPECT = EXPECTED_INSPECT.replace('<<<ID>>>', user.id);
        assert.equal(gotUser.inspect(), EXPECTED_INSPECT);
        clearTimeout(timeout);
        db.close(next);
      });
    });

  });
};