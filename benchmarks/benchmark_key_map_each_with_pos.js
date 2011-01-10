module.exports.run = function(benchmark, next) {
  
  var assert = require('assert');
  var random = require('../tools/random_generator');
  var sys = require('sys') || require('util');
  
  var OBJECT_COUNT = 100000;
  
  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_with_pos_benchmark.alf', function(err, key_map) {
    
    if (err) {
      throw err;
    }

    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }
            key_count ++;
            if (key_count == OBJECT_COUNT) {
              // wait for flush
              setTimeout(function() {

                // test if we can retrieve all keys
                var tested_keys = 0;

                benchmark.start("key_map.each_with_pos for a key map with " +  OBJECT_COUNT + ' elements');

                key_map.each(function(err, key, value, pos, length) {
                  if (err) {
                    throw err;
                  }
                  tested_keys ++;
                  if (tested_keys == key_count) {
                    key_map.end(function(err) {
                      if (err) {
                        throw err;
                      }
                      benchmark.end();
                      next();
                    });
                  }
                });
              }, 1000);
            }
          });
        })(i);
      }
    });
  });
};