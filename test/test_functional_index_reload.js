module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys'),
      key_map_module = require(__dirname + '/../lib/alfred/key_map');


  var file_path = __dirname + '/../tmp/funcitonal_index_reload_test.alf';
  key_map_module.open(file_path, function(err, key_map) {
    
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

    var createRandomObject = function(a, b) {
      return {
        a: a,
        b: b,
        c: createRandomString(random(100))
      };
    };

    var map = {};
    var key_count = 0;
    
    var a_values = [createRandomString(random(100)), createRandomString(random(100)), createRandomString(random(100))];
    var b_values = [createRandomString(random(100)), createRandomString(random(100)), createRandomString(random(100))];
    
    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      for (var i = 0; i < 90; i ++) {
        var value_index = i % 3;
        var value = createRandomObject(a_values[value_index], b_values[value_index]);
        var key = createRandomString(16);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          
          key_count ++;
          if (key_count == 90) {
            // let it flush
            setTimeout(function() {
              
              key_map.end(function(err) {
                if (err) {
                  throw err
                }
                
                key_map_module.open(file_path, function(err, key_map) {
                  if (err) {
                    throw err;
                  }
                  
                  key_map.addIndex('a', function(record) {
                    //console.log(record);
                    return {
                      e: record.a + record.b,
                      f: record.a + record.c,
                    };
                  }, function(err, index) {
                    // done creating the index
                    if (err) {
                      throw err;
                    }
                    var idx = random(3);
                    var looking_for = a_values[idx] + b_values[idx];
                    var selected = 0;

                    var timeout = setTimeout(function() {
                      assert.ok(false, "key_map.filter timeout. Only selected " + selected + " records");
                    }, 10000);

                    key_map.filter('a', function(record) {
                      //console.log('comparing ' + record.e + ' and ')
                      return record.e == looking_for;
                    }, function(err, key, value) {
                      selected ++;
                      if (selected == 30) {
                        clearTimeout(timeout);
                        next();
                      }
                    });
                  });
                  
                });
              });
              
            }, 1000);
          }
        });
      }
    });
  });
}

