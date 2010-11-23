module.exports.run = function(benchmark, next) {
  
  var assert = require('assert');
  var sys    = require('sys') || require('util');
  var fs     = require('fs');
  var indexed_key_map = require(__dirname + '/../lib/alfred/indexed_key_map.js');

  var OBJECT_COUNT = 20000;
  var file_path = __dirname + '/../tmp/indexed_key_map_reload_test.alf';

  fs.unlinkSync(file_path);

  benchmark.start('open empty indexed key map');
  indexed_key_map.open(file_path, function(err, key_map) {

    if (err) {
      throw err;
    }
    
    benchmark.end();
    
    var createRandomString = function(string_length) {
      var chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz";
      var randomstring = '';
      for (var i=0; i<string_length; i++) {
        var rnum = Math.floor(Math.random() * chars.length);
        randomstring += chars.substring(rnum,rnum+1);
      }
      return randomstring;
    };
    
    var createRandomObject = function() {
      return {
        a: createRandomString(10),
        b: createRandomString(100),
        c: createRandomString(100)
      };
    };

    var map = {};
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
        var value = createRandomObject();
        var key = createRandomString(16);
        keys.push(key);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          key_count ++;
          if (key_count == OBJECT_COUNT) {
            benchmark.end();

            setTimeout(function() {

              // test if we can retrieve all keys
              key_map.end(function(err) {
                if (err) {
                  throw err;
                }

                benchmark.start('reopen indexed key map', OBJECT_COUNT);
                indexed_key_map.open(file_path, function(err, key_map) {

                  if (err) {
                    throw err;
                  }

                  benchmark.end();

                  var timeout = setTimeout(function() {
                    assert.ok(false, "timeout");
                  }, 10000)

                  var tested_keys = 0;

                  var going_for = OBJECT_COUNT * 3;

                  benchmark.start('indexed_key_map.get', going_for);

                  for(var j = 0; j < 3; j++) {
                    (function(j) {
                      for (var i = 0; i < OBJECT_COUNT; i++) {
                        (function(i) {
                          var key = keys[i];
                          var value = map[key];
                          key_map.get(key, function(err, record) {
                            assert.deepEqual(record, value);
                          });
                          tested_keys ++;
                          if (tested_keys == going_for) {
                            benchmark.end();
                            clearTimeout(timeout);
                            next();
                          }
                        })(i);
                      }
                    })(j);
                  }

                });

              });

            }, 2000);
          }
        });
      }

      // wait for flush

    });

  });
  

}

