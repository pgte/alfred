module.exports.run = function(next) {
  
  var assert = require('assert');
  var sys    = require('sys') || require('util');
  var fs    = require('fs');
  var random = require('../tools/random_generator');

  var TEST_KEYS_NUMBER = 10;
  var OBJECT_COUNT = 1000;
  
  var file_path = __dirname + '/../tmp/indexed_key_map_test.alf';

  try {
    fs.unlinkSync(file_path);
  } catch(excp) {
    // do nothing
  }
  require(__dirname + '/../lib/alfred/indexed_key_map.js').open(file_path, function(err, key_map) {
    
    if (err) {
      throw err;
    }
    
    var map = {};
    var keys = [];
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          keys.push(key);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }
            key_count ++;

            if (key_count == OBJECT_COUNT) {

              // test if we can retrieve all keys
              var timeout = setTimeout(function() {
                assert.ok(false, "timeout");
              }, 10000);

              var tested_keys = 0;

              for (var i = 0; i < TEST_KEYS_NUMBER; i++) {
                (function(i) {
                  var key = keys[Math.floor(Math.random() * key_count)];
                  var value = map[key];
                  assert.ok(!!value);
                  key_map.get(key, function(err, record) {
                    if (err) { throw err; }
                    assert.deepEqual(record, value);
                  });
                  tested_keys ++;
                  if (tested_keys == TEST_KEYS_NUMBER) {

                    key_map.count(function(err, count) {
                      if (err) {
                        throw err;
                      }
                      assert.equal(OBJECT_COUNT, count, "key_map count result (" + count + ") is different from inserted keys count (" + OBJECT_COUNT + ")");

                      key_map.end(function(err) {
                        if (err) {
                          throw err;
                        }
                        clearTimeout(timeout);
                        next();
                      });
                    });
                  }
                })(i);
              }
            }
          });
        })(i);
      }
    });
  });
};