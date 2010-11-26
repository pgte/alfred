module.exports.run = function(benchmark, next) {
  
  var random = require('../tools/random_generator');
  var OBJECT_COUNT= 1000;
  
  benchmark.start('opening collection');
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_bench.alf', function(err, collection) {
    if (err) {
      throw err;
    }
    
    benchmark.end();
    var assert = require('assert');

    benchmark.start('prunning collection');
    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      benchmark.end();
      benchmark.start('populating collection', OBJECT_COUNT);
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          record = random.createRandomObject();
          collection.write(record, function(err) {
            if (err) {
              throw err;
            }
          });
        })(i);
      }
      benchmark.end();

      var index = 0;
      benchmark.start('reading entire collection of ' + OBJECT_COUNT + ' records');
      collection.read(function(error, record) {
        assert.equal(error, null);
        if (record === null) {
          benchmark.end();
          collection.end(function(err) {
            if (err) {
              throw err;
            }
            next();
          });
        } else {
          index ++;
        }
      }, true);
      
    });
  });
};