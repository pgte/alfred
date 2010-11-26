module.exports.run = function(next) {
  
  var assert = require('assert');
  var random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_test.alf', function(err, key_map) {
    
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
            if (err) {
              throw err;
            }
            key_count ++;
            if (key_count == 100) {
              // test if we can retrieve all keys
              var tested_keys = 0;

              var timeout = setTimeout(function() {
                assert.ok(false, 'timeout');
              }, 10000);

              key_map.each(function(err, key, value) {
                assert.deepEqual(map[key], value);
                tested_keys ++;
                if (tested_keys == 100) {
                  key_map.end(function(err) {
                    if (err) {
                      throw err;
                    }
                    clearTimeout(timeout);
                    next();
                  });
                }
              });
            }
          });
        })(i);
      }
    });
  });
};