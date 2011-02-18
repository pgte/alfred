module.exports.run = function(next) {
  
  var assert = require('assert'),
      sys    = require('sys') || require('util'),
      fs     = require('fs'),
      random = require('../tools/random_generator');
  
  var WRITE_COUNT = 100;
  var CONCURRENCY = 1000;
  
  var file_path = __dirname + '/../tmp/atomic_null_test.alf';
      
  require(__dirname + '/../lib/alfred/indexed_key_map.js').open(file_path, function(err, key_map) {

    if (err) { next(err); return; }

    var keys = [];

    key_map.clear(function(err) {
      if (err) { next(err); return; }
      
      var read_count = 0;
      
      var timeout = setTimeout(function() {
        assert.ok(false, 'timeout');
      }, 60000);
      
      key_map.atomic('gugu', function(err, value) {
        if (err) { next(err); return; }
        return {a:10};
      }, function(err) {
        if (err) { next(err); return; }
        key_map.get('gugu', function(err, value) {
          if (err) { next(err); return; }
          assert.deepEqual(value, {a:10});
          key_map.end(next);
        });
      });
      
    });
  });
};