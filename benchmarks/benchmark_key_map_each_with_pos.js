module.exports.run = function(benchmark, next) {
  
  var assert = require('assert');
  var sys = require('sys') || require('util');
  
  var OBJECT_COUNT = 100000;
  
  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_with_pos_benchmark.alf', function(err, key_map) {
    
    if (err) {
      throw err;
    }

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
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        var value = createRandomObject();
        var key = createRandomString(16);
        map[key] = value;
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
                  benchmark.end();
                  next();
                }

              });


            }, 1000);
          }
        });
      }


    });

    
  });
  

}

