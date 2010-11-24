module.exports.run = function(next) {
  
  var assert = require('assert');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_test.alf', function(err, key_map) {
    
    if (err) {
      throw err;
    }

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

    var createRandomObject = function() {
      return {
        a: createRandomString(random(10)),
        b: createRandomString(random(100)),
        c: createRandomString(random(100))
      };
    };

    var map = {};
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < 100; i ++) {
        var value = createRandomObject();
        var key = createRandomString(16);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          key_count ++;
          if (key_count == 100) {
            // wait for flush
            setTimeout(function() {

              // test if we can retrieve all keys
              var tested_keys = 0;

              key_map.each(function(err, key, value) {
                assert.deepEqual(map[key], value);
                tested_keys ++;
              });

              setTimeout(function() {
                assert.equal(key_count, tested_keys, 'tested keys is not equal to original key count');
                next();
              }, 3000);

            }, 1000);

            
          }
        });
      }

    });

    
  });
  

}

