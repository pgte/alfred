module.exports.run = function(benchmark, next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      random = require('../tools/random_generator'),
      key_map_module = require(__dirname + '/../lib/alfred/cached_key_map');
      
  var OBJECT_COUNT = 90000;


  var file_path = __dirname + '/../tmp/cached_functional_index_benchmark.alf';
  benchmark.start('open key map');
  key_map_module.open(file_path, function(err, key_map) {
    
    if (err) {
      throw err;
    }
    
    benchmark.end();

    var createRandomObject = function(a, b) {
      return {
        a: a,
        b: b,
        c: random.createRandomString(random.random(100))
      };
    };

    var key_count = 0;
    
    benchmark.start('clearing key map');
    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      
      benchmark.end();
      
      var a_values = [random.createRandomString(random.random(100)), random.createRandomString(random.random(100)), random.createRandomString(random.random(100))];
      var b_values = [random.createRandomString(random.random(100)), random.createRandomString(random.random(100)), random.createRandomString(random.random(100))];
      
      benchmark.start('populating key map', OBJECT_COUNT);
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value_index = i % 3;
          var value = createRandomObject(a_values[value_index], b_values[value_index]);
          var key = random.createRandomString(16);
          key_map.put(key, value, function(err) {
            if (err) {
              throw err;
            }

            key_count ++;
            if (key_count == OBJECT_COUNT) {

              benchmark.end();
              // let it flush
              benchmark.start('ending key map');
              key_map.end(function(err) {
                if (err) {
                  throw err;
                }

                benchmark.end();

                benchmark.start('reopening key map with ' + OBJECT_COUNT + ' entries');
                key_map_module.open(file_path, function(err, key_map) {
                  if (err) {
                    throw err;
                  }

                  benchmark.end();

                  benchmark.start('adding index to key map with ' + OBJECT_COUNT + ' entries');
                  key_map.addIndex('a', function(record) {
                    //console.log(record);
                    return {
                      e: record.a + record.b,
                      f: record.a + record.c
                    };
                  }, function(err, index) {
                    // done creating the index
                    if (err) {
                      throw err;
                    }

                    benchmark.end();

                    var timeout = setTimeout(function() {
                      assert.ok(false, "key_map.filter timeout 1");
                    }, 15000);

                    benchmark.start("scan index with " + OBJECT_COUNT + ' entries');

                    key_map.filter('a', function(record) {
                      return false;
                    }, function(err, key, value) {

                      if (err) {
                        throw err;
                      }

                      clearTimeout(timeout);
                      benchmark.end();
                      assert.equal(null, key);

                      var idx = random.random(3);
                      var looking_for = a_values[idx] + b_values[idx];
                      var selected = 0;
                      var expect_count = OBJECT_COUNT / 3;

                      timeout = setTimeout(function() {
                        assert.ok(false, "key_map.filter timeout 2");
                      }, 60000);

                      benchmark.start('filter and get ' + expect_count + ' records from a key map with ' + OBJECT_COUNT + ' elements');
                      
                      key_map.filter('a', function(record) {
                        return record.e == looking_for;
                      }, function(err, key, value) {
                        if (err) {
                          throw err;
                        }
                        selected ++;
                        if (selected == expect_count) {
                          benchmark.end();
                          key_map.end(function(err) {
                            if (err) {
                              throw err;
                            }
                            clearTimeout(timeout);
                            next();
                          });
                        }
                      });
                    }, true);
                  });
                });
              });
            }
          });
        })(i);
      }
    });
  });
};