module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys');
  var random = require('../tools/random_generator');
  var KeyMap = require(__dirname + '/../lib/alfred/key_map.js');
  
  DB_PATH = __dirname + '/../tmp/key_map_reload_test.alf';

  KeyMap.open(DB_PATH, function(err, key_map) {
    
    if (err) {
      throw err;
    }

    var map = {};
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < 100; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) { throw err; }
            key_count ++;
            if (key_count == 100) {

                // test if we can retrieve all keys
              var tested_keys = 0;
              var tested_null = false;
              
              key_map.end(function(err) {
                if (err) { throw err; }
                
                var timeout = setTimeout(function() {
                  assert.ok(false, 'timeout');
                }, 5000);
                
                KeyMap.open(DB_PATH, function(err, key_map) {
                  clearTimeout(timeout);
                  next();
                });
                
              });

            }
          });
        })(i);
      }
    });
  });
};