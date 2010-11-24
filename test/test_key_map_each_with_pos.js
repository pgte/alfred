module.exports.run = function(next) {
  
  var assert = require('assert');
  var sys = require('sys') || require('util');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_each_with_pos_test.alf', function(err, key_map) {
    
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
      for (var i = 0; i < 1000; i ++) {
        var value = createRandomObject();
        var key = createRandomString(16);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          key_count ++;
          
          if (key_count == 1000) {
            // wait for flush
            setTimeout(function() {

              // test if we can retrieve all keys
              var tested_keys = 0;

              key_map.each(function(err, key, value, pos, length) {
                if (err) {
                  throw err;
                }
                assert.deepEqual(map[key], value);
                key_map.get_at_pos(pos, length, function(err, ret_key, ret_value) {
                  if (err) {
                    throw err;
                  }
                  assert.equal(key, ret_key, "iteration key (" + key + ") is not the same as one got from get_at_pos (" + ret_key + ")");
                  assert.deepEqual(value, ret_value, "iteration value (" + value + ") is not the same as one got from get_at_pos (" + ret_value + ")");
                  tested_keys ++;
                });
              });

              setTimeout(function() {
                assert.equal(key_count, tested_keys, 'tested key count (' + tested_keys + ') is not equal to original key count (' + key_count + ')');
                next();
              }, 3000);

            }, 1000);
            
          }
        });
      }


    });

    
  });
  

}

