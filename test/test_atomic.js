module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys') || require('util'),
      fs     = require('fs'),
      random = require('../tools/random_generator');
  
  var WRITE_COUNT = 100;
  var CONCURRENCY = 1000;
  
  var file_path = __dirname + '/../tmp/atomic_test.alf';
      
  require(__dirname + '/../lib/alfred/indexed_key_map.js').open(file_path, function(err, key_map) {

    if (err) { next(err); return; }

    var keys = [];

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      
      var read_count = 0;
      
      var timeout = setTimeout(function() {
        assert.ok(false, 'timeout');
      }, 60000);
      
      var written = 0;
      for(var i=0; i < WRITE_COUNT; i++) {
        (function(i) {
          var obj = random.createRandomObject();
          var key = random.createRandomString(16);
          key_map.put(key, obj, function(err) {
            if (err) { next(err); return; }
            keys.push(key);
            written ++;
            if (written == WRITE_COUNT) {
              // done
              var random_key = keys[Math.floor(Math.random() * keys.length)];
              
              var atomics_done = 0;
              for(var j=0; j < CONCURRENCY; j ++) {
                (function(j) {
                  key_map.atomic(random_key, function(err, value) {
                    if (err) { next(err); return; }
                    assert.notEqual(value, null);
                    if (!value.counter) {
                      value.counter = 0;
                    }
                    value.counter ++;
                    return value;
                  }, function(err) {
                    if (err) { next(err); return; }
                    atomics_done ++;
                    if (atomics_done == CONCURRENCY) {
                      key_map.get(random_key, function(err, value) {
                        if (err) { next(err); return; }
                        assert.equal(value.counter, CONCURRENCY);
                        clearTimeout(timeout);
                        key_map.end(function(err) {
                          if (err) { next(err); return; }
                          next();
                        });
                      });
                    }
                  });
                })(j);
              }
            }
          });
        })(i);
      }
    });
  });
};