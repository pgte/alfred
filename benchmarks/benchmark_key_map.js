module.exports.run = function(benchmark, next) {

  var assert = require('assert');
  var random = require('../tools/random_generator');
  
  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_benchmark.alf', {buffered: true}, function(err, key_map) {
    if (err) {
      throw err;
    }

    var KEY_COUNT = 50000;
    var RETRIEVE_RANDOM_COUNT = 10; //0;

    var keys = [];
    var map = {};

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      benchmark.start('Insert object into key map', KEY_COUNT);
      for (var i = 0; i < KEY_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          keys.push(key);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }
            written ++;
            if (written == KEY_COUNT) {
              benchmark.end();
              // wait for flush
              setTimeout(function() {

                var sample_key_index = Math.floor(Math.random() * KEY_COUNT);
                var key = keys[sample_key_index];
                var retrieved_keys = 0;

                benchmark.start('Retrieve a random object by key from a ' + KEY_COUNT + ' hash map', RETRIEVE_RANDOM_COUNT);
                for(var i = 0; i < RETRIEVE_RANDOM_COUNT; i++) {
                  (function(i) {
                    key_map.get(key, function(err, record) {
                      if (err) {
                        throw err;
                      }
                      retrieved_keys ++;
                      if (retrieved_keys == RETRIEVE_RANDOM_COUNT) {
                        key_map.end(function(err) {
                          if (err) {
                            throw err;
                          }
                          benchmark.end();
                          next();
                        });
                      }
                    });
                  })(i);
                }
              }, 1000);
            }
          });
        })(i);
      }
    });
  });
};