module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys') || require('util'),
      fs     = require('fs'),
      random = require('../tools/random_generator');
  
  var file_path = __dirname + '/../tmp/test_compact_test.alf';
      
  require(__dirname + '/../lib/alfred/indexed_key_map.js').open(file_path, function(err, key_map) {

    if (err) { next(err); return; }
    
    key_map.clear(function(err) {
      if (err) { next(err); return; }
      
      var written_keys = {};
      var written_count = 0;

      var interval = setInterval(function() {
        var obj = random.createRandomObject();
        var key = random.createRandomString(16);
        key_map.put(key, obj, function(err) {
          if (err) { next(err); return; }
          written_count ++;
          written_keys[key] = obj;
        });
      }, 50);
      
      setTimeout(function() {
        clearInterval(interval);
        setTimeout(function() {
          // rewrite every object again
          var rewritten_keys = 0;
          for(var key in written_keys) {
            (function(key) {
              var new_obj = random.createRandomObject();
              key_map.put(key, new_obj, function(err) {
                if (err) { next(err); return; }
                written_keys[key] = new_obj;
                rewritten_keys ++;
                if (rewritten_keys == written_count) {
                  
                  // keep writing more keys
                  interval = setInterval(function() {
                    var obj = random.createRandomObject();
                    var key = random.createRandomString(24);
                    
                    if (written_keys[key]) {
                      next(new Error('generated key already exists'));
                      return;
                    }
                    
                    key_map.put(key, obj, function(err) {
                      if (err) { next(err); return; }
                      written_count ++;
                      written_keys[key] = obj;
                      var the_count = 0;
                      for(var k in written_keys) {
                        the_count ++;
                      }
                    });
                  }, 50);
                  
                  setTimeout(function() {
                    key_map.compact(function(err) {
                      if (err) { next(err); return; }
                      clearInterval(interval);
                      var read_count = 0;
                      for(var key in written_keys) {
                        (function(key) {
                          key_map.get(key, function(err, value) {
                            if (err) { next(err); return; }
                            assert.deepEqual(value, written_keys[key]);
                            read_count ++;
                            if (read_count == written_count) {
                              key_map.end(function(err) {
                                if (err) { next(err); return; }
                                next();
                              });
                            }
                          });
                        })(key);
                      }
                    });
                  }, 2000);
                  
                }
              })
            })(key);
          };
        }, 1000);
      }, 2000);
      
      setTimeout(function() {
      }, 2000)

      var keys = [];

    });
  });
};