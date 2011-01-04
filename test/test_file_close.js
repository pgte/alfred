module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys');
  var random = require('../tools/random_generator');
  
  var OBJECT_COUNT = 100;

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/file_close_test.alf', {buffered: true}, function(err, key_map) {
    
    if (err) { next(err); return; }

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }
          });
        })(i);
      }
      
      key_map.end(function(err) {
        if (err) { next(err); return; }
        next();
      });
      
    });
  });
};