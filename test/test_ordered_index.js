module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/funcitonal_index_test.alf', function(err, key_map) {
    
    if (err) { next(err); return; }

    var createRandomObject = function(a, b) {
      return {
        a: a,
        b: b,
        c: random.createRandomString(random.random(100))
      };
    };

    var map = {};
    var key_count = 0;
    
    var a_values = [random.createRandomString(random.random(100)), random.createRandomString(random.random(100)), random.createRandomString(random.random(100))];
    var b_values = [random.createRandomString(random.random(100)), random.createRandomString(random.random(100)), random.createRandomString(random.random(100))];
    
    key_map.clear(function(err) {
      if (err) { next(err); return; }
      for (var i = 0; i < 90; i ++) {
        (function(i) {
          var value_index = i % 3;
          var value = createRandomObject(a_values[value_index], b_values[value_index]);
          var key = random.createRandomString(16);
          map[key] = value;
          key_map.put(key, value, function(err) {
            if (err) { next(err); return; }

            key_count ++;
            if (key_count == 90) {
              key_map.addIndex('a', {ordered: true}, function(record) {
                //console.log(record);
                return record.a + record.b;
              }, function(err, index) {
                if (err) { next(err); return; }
                // done creating the index
                var idx = random.random(3);
                var looking_for = a_values[idx] + b_values[idx];
                var selected = 0;

                var timeout = setTimeout(function() {
                  assert.ok(false, "key_map.filter timeout. Only selected " + selected + " records");
                }, 10000);

                var filter_function = function(record) {
                  return record == looking_for;
                };
                
                key_map.filter('a', filter_function, function(err, key, value) {
                  if (err) { next(err); return; }
                  selected ++;
                  if (selected == 30) {
                    
                    key_map.countFilter('a', filter_function, function(err, count) {

                      assert.equal(selected, count, "key_map.countFilter results (" + count + ") is different from previously selected count (" + selected + ")");

                      key_map.end(function(err) {
                        if (err) { next(err); return; }
                        clearTimeout(timeout);
                        next();
                      });
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