module.exports.run = function(next) {
  
  var assert = require('assert');
  var sys    = require('sys') || require('util');
  var fs    = require('fs');
  var random = require('../tools/random_generator');

  var DIFFERENT_OBJECT_COUNT = 5;
  var OBJECT_COUNT = 1000;
  
  var file_path = __dirname + '/../tmp/cached_key_map_test.alf';

  try {
    fs.unlinkSync(file_path);
  } catch(excp) {
    // do nothing
  }
  require(__dirname + '/../lib/alfred/cached_key_map.js').open(file_path, function(err, key_map) {
    
    if (err) {
      throw err;
    }

    var count = 0;
    var values = [];
    var interesting = 0;

    for (var i = 0; i < DIFFERENT_OBJECT_COUNT; i ++) {
      values.push(random.createRandomString(16));
    }

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      
      var got_interesting = 0;
      var filter = function(key, value) {
        return value == values[0];
      };
      
      key_map.startStream(filter, function(key, value) {
        assert.equal(value, values[0]);
        got_interesting ++;
      });
      
      key_map.on('error', function(err) {
        throw err;
      });
      
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        var key = random.createRandomString(16);
        var value = values[Math.floor(Math.random() * values.length)];
        if (value == values[0]) {
          interesting ++;
        }
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          count ++;
          
          if (count == OBJECT_COUNT) {
            // test if we can retrieve all keys

            var timeout = setTimeout(function() {
              assert.equal(interesting, got_interesting);
              next();
            }, 1000);

          }
        });
      }
    });
  });
};

