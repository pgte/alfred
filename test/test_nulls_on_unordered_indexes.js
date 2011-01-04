module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      random = require('../tools/random_generator');
      
  var OBJECT_COUNT = 90;

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/nulls_on_unordered_index_test.alf', function(err, key_map) {
    
    if (err) { next(err); return; }

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      
      var map = {};
      var key_count = 0;
      
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value_index = i % 3;
          var value = random.createRandomObject();
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }

            key_count ++;
            if (key_count == OBJECT_COUNT) {
              
              key_map.addIndex('a', {orderered: false}, function(record) {
                return record.a + record.b;
              }, function(err) {
                if (err) { next(err); return; }
                var got = 0;

                for(key in map) {
                  (function(key) {
                    if (map.hasOwnProperty(key)) {
                      var here_value = map[key];
                      var expected_value = here_value.a + here_value.b;
                      key_map.indexMatch('a', expected_value, function(err, ret_key, value) {
                        if (err) { next(err); return; }
                        assert.equal(key, ret_key);
                        got ++;
                        if (got == OBJECT_COUNT) {
                          // finished first match
                          got = 0;
                          for(key in map) {
                            (function(key) {
                              var here_value = map[key];
                              var expected_value = here_value.a + here_value.b;
                              key_map.put(key, null, function(err) {
                                if (err) { next(err); return; }
                                key_map.indexMatch('a', expected_value, function(err, key, value) {
                                  if (err) { next(err); return; }
                                  assert.equal(value, null);
                                  assert.equal(key, null);
                                  got ++;
                                  if (got == OBJECT_COUNT) {
                                    key_map.end(function(err) {
                                      if (err) { next(err); return; }
                                      next();
                                    })
                                  }
                                });
                              })
                            })(key);
                          }
                        }
                      });
                    };
                  })(key);
                };
                
              });
              
            }
          });
        })(i);
      }
    });
  });
};