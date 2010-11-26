module.exports.run = function(benchmark, next) {

  var random = require('../tools/random_generator');
  var assert = require('assert');
  
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_benchmark.alf', function(err, collection) {
    if (err) {
      throw err;
    }
    var OBJECT_COUNT = 100000;

    var createRandomObject = function(c_value) {
      return {
        a: random.createRandomString(10),
        b: random.createRandomString(50),
        c: c_value
      };
    };

    var c_values = [random.createRandomString(random.random(50)), random.createRandomString(random.random(50)), random.createRandomString(random.random(50))];

    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          record = createRandomObject(c_values[i % c_values.length]);
          collection.write(record, function(err) {
            if (err) {
              throw err;
            }
            written ++;
            if (written == OBJECT_COUNT) {
              var index = 0;
              benchmark.start('Collection filter of collection with ' + OBJECT_COUNT + ' records');
              collection.filter(function(record) {
                return record.c == c_values[1];
              }, function(error, result) {
                if (error) {
                  throw error;
                }
                if (result === null) {
                  benchmark.end();
                  collection.end(function(err) {
                    if (err) {
                      throw err;
                    }
                    next();
                  });
                }
              });
            }
          });
        })(i);
      }
    });
  });
};