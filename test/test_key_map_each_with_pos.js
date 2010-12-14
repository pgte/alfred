module.exports.run = function(next) {
  
  var assert = require('assert');
  var sys = require('sys') || require('util');
  var random = require('../tools/random_generator');

  var OBJECT_COUNT = 10;

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_with_pos_test.alf', function(err, key_map) {
    
    if (err) {
      throw err;
    }

    var map = {};
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }
            key_count ++;

            if (key_count == OBJECT_COUNT) {
              // test if we can retrieve all keys
              var tested_keys = 0;
              var timeout = setTimeout(function() {
                assert.equal(key_count, tested_keys, 'tested key count (' + tested_keys + ') is not equal to original key count (' + key_count + ')');
                next();
              }, 10000);


              key_map.each(function(err, key, value, pos, length) {
                if (err) {
                  throw err;
                }
                assert.deepEqual(map[key], value);
                key_map.get_at_pos(pos, length, function(err, ret_key, ret_value) {
                  if (err) {
                    throw err;
                  }
                  assert.equal(key, ret_key, "iteration key (" + key + ") is not the same as one got from get_at_pos (" + ret_key + ")");
                  assert.deepEqual(value, ret_value, "iteration value (" + value + ") is not the same as one got from get_at_pos (" + ret_value + ")");
                  tested_keys ++;
                  if (tested_keys == OBJECT_COUNT) {
                    key_map.end(function(err) {
                      if (err) {
                        throw err;
                      }
                      clearTimeout(timeout);
                      next();
                    });
                  }
                });
              });
            }
          });
        })(i);
      }
    });
  });
};