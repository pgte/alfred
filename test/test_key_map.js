module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys');
  var random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_test.alf', function(err, key_map) {
    
    if (err) { next(err); return; }

    var map = {};
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      for (var i = 0; i < 100; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }
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
                if (map.hasOwnProperty(key)) {
                  (function(key) {
                    key_map.get(key, function(error, result) {
                      if (err) { next(err); return; }
                      if (result === null) {
                        console.log('did not find for key ' + key + '. Already tested ' + tested_keys);
                      }
                      assert.deepEqual(map[key], result);
                      tested_keys ++;
                      if (tested_keys == 100) {

                        // test if the result of a non-existing key is null
                        key_map.get(random.createRandomString(20), function(error, result) {
                          if (err) { next(err); return; }
                          assert.equal(result, null);

                          // end test
                          key_map.end(function(err) {
                            if (err) { next(err); return; }
                            clearTimeout(timeout);
                            next();
                          });

                        });
                      }
                    });
                  })(key);
                }
              }
            }
          });
        })(i);
      }
    });
  });
};