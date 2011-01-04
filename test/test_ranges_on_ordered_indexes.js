module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      random = require('../tools/random_generator');
      
  var OBJECT_COUNT = 90;

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/ranges_on_ordered_index_test.alf', function(err, key_map) {
    
    if (err) { next(err); return; }

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      
      var createRandomObject = function(order) {
        return {
          a: order,
          b: random.createRandomString(random.random(50)),
          c: random.createRandomString(random.random(50))
        }
      };
      
      var key_count = 0;
      
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var value_index = i % 3;
          var value = createRandomObject(i);
          var key = i;
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }

            key_count ++;
            if (key_count == OBJECT_COUNT) {
              
              key_map.addIndex('a', {ordered: true, bplustree_order: 10}, function(record) {
                return record.a;
              }, function(err) {
                if (err) { next(err); return; }
                var got = 0;
                
                var timeout = setTimeout(function() {
                  assert.ok(false, 'timeout');
                }, 10000);
                
                key_map.range('a', 30, 60, function(err, key, value) {
                  if (err) { next(err); return; }
                  assert.ok(value.a >= 30, 'value.a >= 30');
                  assert.ok(value.a <= 60, 'value.a <= 30');
                  got ++;
                  if (got == 30) {
                    clearTimeout(timeout);
                    key_map.end(function(err) {
                      if (err) { next(err); return; }
                      next();
                    });
                  }
                });
              });
            }
          });
        })(i);
      }
    });
  });
};