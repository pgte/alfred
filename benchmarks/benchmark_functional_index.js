module.exports.run = function(benchmark, next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      key_map_module = require(__dirname + '/../lib/alfred/key_map');
      
  var OBJECT_COUNT = 90000;


  var file_path = __dirname + '/../tmp/functional_index_benchmark.alf';
  benchmark.start('open key map');
  key_map_module.open(file_path, function(err, key_map) {
    
    if (err) {
      throw err;
    }
    
    benchmark.end();

    var createRandomString = function(string_length) {
      if (string_length == 0) {
        string_length = 3;
      }
      var chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz";
      var randomstring = '';
      for (var i=0; i<string_length; i++) {
        var rnum = Math.floor(Math.random() * chars.length);
        randomstring += chars.substring(rnum,rnum+1);
      }
      return randomstring;
    };

    var random = function(max) {
      return Math.floor(Math.random() * max);
    };

    var createRandomObject = function(a, b) {
      return {
        a: a,
        b: b,
        c: createRandomString(random(100))
      };
    };

    var map = {};
    var key_count = 0;
    
    benchmark.start('clear key map');
    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      
      benchmark.end();
      
      var a_values = [createRandomString(random(100)), createRandomString(random(100)), createRandomString(random(100))];
      var b_values = [createRandomString(random(100)), createRandomString(random(100)), createRandomString(random(100))];
      
      benchmark.start('populate key map', OBJECT_COUNT);
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        var value_index = i % 3;
        var value = createRandomObject(a_values[value_index], b_values[value_index]);
        var key = createRandomString(16);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          
          key_count ++;
          if (key_count == OBJECT_COUNT) {
            
            benchmark.end();
            // let it flush
            setTimeout(function() {
              
              benchmark.start('end key map');
              key_map.end(function(err) {
                if (err) {
                  throw err
                }
                
                benchmark.end();
                
                benchmark.start('reopen key map with ' + OBJECT_COUNT + ' elements');
                key_map_module.open(file_path, function(err, key_map) {
                  if (err) {
                    throw err;
                  }
                  
                  benchmark.end();
                  
                  benchmark.start('add index to key map with ' + OBJECT_COUNT + ' elements');
                  key_map.addIndex('a', function(record) {
                    //console.log(record);
                    return {
                      e: record.a + record.b,
                      f: record.a + record.c,
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
                    
                    benchmark.start("scan index of key map with " + OBJECT_COUNT + ' elements');

                    key_map.filter('a', function(record) {
                      return false;
                    }, function(err, key, value) {
                      
                      if (err) {
                        throw err;
                      }
                      
                      clearTimeout(timeout);
                      benchmark.end();
                      assert.equal(null, key);
                      
                      var idx = random(3);
                      var looking_for = a_values[idx] + b_values[idx];
                      var selected = 0;
                      var expect_count = OBJECT_COUNT / 3;

                      timeout = setTimeout(function() {
                        assert.ok(false, "key_map.filter timeout 2");
                      }, 15000);

                      benchmark.start("scan and get " + expect_count + "elements from a key map with " + OBJECT_COUNT + 'elements', expect_count);
                      
                      key_map.filter('a', function(record) {
                        return record.e == looking_for;
                      }, function(err, key, value) {
                        if (err) {
                          throw err;
                        }
                        selected ++;
                        if (selected == expect_count) {
                          benchmark.end();
                          clearTimeout(timeout);
                          next();
                        }
                        
                      });
                      
                    }, true);
                  });
                  
                });
              });
              
            }, 1000);
          }
        });
      }
    });
  });
}

