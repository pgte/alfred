module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys');
  var random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_test.alf', function(err, key_map) {
    
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
            var tested_null = false;

            var timeout = setTimeout(function() {
              assert.ok(false, "timeout");
            }, 10000);

            // test than when we try to retrieve with a non-existing ket, we get null back
            for(var key in map) {
              (function(key) {
                key_map.get(key, function(error, result) {
                  if (error) {
                    throw error;
                  }
                  if (result == null) {
                    console.log('did not find for key ' + key + '. Already tested ' + tested_keys);
                  }
                  assert.deepEqual(map[key], result);
                  tested_keys ++;
                  if (tested_keys == 100) {
                    key_map.get(random.createRandomString(20), function(error, result) {
                      if (error) {
                        throw error;
                      }
                      assert.equal(result, null);
                      key_map.end(function(err) {
                        if (err) {
                          throw err;
                        }
                        clearTimeout(timeout);
                        next();
                      });
                    });
                  }
                });
              })(key);
            }
          }
        });
      }


    });

    
  });
  

}

