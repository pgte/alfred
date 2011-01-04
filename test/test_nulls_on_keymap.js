module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/indexed_key_map.js').open(__dirname + '/../tmp/nulls_on_keymap_test.alf', function(err, key_map) {
    
    if (err) { next(err); return; }
    
    var map = {};
    var key_count = 0;
    
    key_map.clear(function(err) {
      if (err) { next(err); return; }
      for (var i = 0; i < 90; i ++) {
        (function(i) {
          var value_index = i % 3;
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }

            key_count ++;
            if (key_count == 90) {
              
              var got = 0;
              
              for(key in map) {
                if (map.hasOwnProperty(key)) {
                  key_map.put(key, null, function(err) {
                    if (err) { next(err); return; }

                    key_map.get(key, function(err, value) {
                      if (err) { next(err); return; }
                      assert.ok(value === null, 'value was not null after setting it to null');
                      got ++;
                      if (got == 90) {
                        
                        key_map.end(function(err) {
                          if (err) { next(err); return; }
                          next();
                        });
                      }
                    });
                  });
                };
              };
            }
          });
        })(i);
      }
    });
  });
};