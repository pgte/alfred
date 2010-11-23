module.exports.run = function(next) {
  
  var assert = require('assert');
  var sys    = require('sys') || require('util');
  var fs     = require('fs');
  var indexed_key_map = require(__dirname + '/../lib/alfred/indexed_key_map.js');

  var TEST_KEYS_NUMBER = 10;
  var file_path = __dirname + '/../tmp/indexed_key_map_reload_test.alf';

  fs.unlinkSync(file_path);

  indexed_key_map.open(file_path, function(err, key_map) {

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
    var keys = [];
    var key_count = 0;

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < 1000; i ++) {
        var value = createRandomObject();
        var key = createRandomString(16);
        keys.push(key);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          key_count ++;
        });
      }

      // wait for flush
      setTimeout(function() {

        // test if we can retrieve all keys
        key_map.end();
        
        indexed_key_map.open(file_path, function(err, key_map) {
          var timeout = setTimeout(function() {
            assert.ok(false, "timeout");
          }, 10000)

          var tested_keys = 0;

          for (var i = 0; i < TEST_KEYS_NUMBER; i++) {
            var key = keys[Math.floor(Math.random() * key_count)];
            var value = map[key];
            assert.ok(!!value);
            key_map.get(key, function(err, record) {
              assert.deepEqual(record, value);
            });
            tested_keys ++;
            if (tested_keys == TEST_KEYS_NUMBER) {
              clearTimeout(timeout);
              next();
            }
          }
        });
        
      }, 2000);

    });

  });
  

}

