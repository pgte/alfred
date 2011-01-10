module.exports.run = function(benchmark, next) {
  
  var sys    = require('sys') || require('util');
  var fs     = require('fs');
  var random = require('../tools/random_generator');
  var indexed_key_map = require(__dirname + '/../lib/alfred/indexed_key_map.js');

  var OBJECT_COUNT = 100000;
  var file_path = __dirname + '/../tmp/indexed_key_map_reload_test.alf';

  try {
    fs.unlinkSync(file_path);
  } catch(excp) {
    // do nothing
  }

  benchmark.start('open empty indexed key map');
  indexed_key_map.open(file_path, function(err, key_map) {

    if (err) {
      throw err;
    }
    
    benchmark.end();
    
    var keys = [];
    var key_count = 0;
    
    benchmark.start('clear empty indexed key map');
    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      benchmark.end();
      
      benchmark.start('populate indexed key map', OBJECT_COUNT);
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          keys.push(key);
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }
            key_count ++;
            if (key_count == OBJECT_COUNT) {
              benchmark.end();

              // test if we can retrieve all keys
              key_map.end(function(err) {
                if (err) {
                  throw err;
                }
                
                benchmark.start('reopen indexed key map with ' + OBJECT_COUNT + ' elements');
                indexed_key_map.open(file_path, function(err, key_map) {
                  
                  if (err) {
                    throw err;
                  }

                  benchmark.end();

                  var timeout = setTimeout(function() {
                    throw new Error('timeout');
                  }, 240000);

                  var tested_keys = 0;

                  var going_for = OBJECT_COUNT * 3;

                  benchmark.start('indexed_key_map.get', going_for);

                  for(var j = 0; j < 3; j++) {
                    (function(j) {
                      for (var i = 0; i < OBJECT_COUNT; i++) {
                        (function(i) {
                          var key = keys[i];
                          key_map.get(key, function(err, record) {
                            if (err) { throw err; }
                          });
                          tested_keys ++;
                          if (tested_keys == going_for) {
                            benchmark.end();
                            key_map.end(function(err) {
                              if (err) {
                                throw err;
                              }
                              clearTimeout(timeout);
                              next();
                            });
                          }
                        })(i);
                      }
                    })(j);
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