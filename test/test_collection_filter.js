module.exports.run = function(next) {
  var assert = require('assert');
  var random = require('../tools/random_generator');
  
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_test.alf', function(err, collection) {

    if (err) {
      throw err;
    }

    var OBJECT_COUNT = 15000;

    var createRandomObject = function(c_value) {
      return {
        a: random.createRandomString(random.random(10)),
        b: random.createRandomString(random.random(100)),
        c: c_value
      };
    };

    var c_values = [random.createRandomString(random.random(100)), random.createRandomString(random.random(100)), random.createRandomString(random.random(100))];

    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        (function(i) {
          var record = createRandomObject(c_values[i % c_values.length]);
          collection.write(record, function(err) {
            if (err) {
              throw err;
            }
            written ++;
            if (written == OBJECT_COUNT) {
              var result_count = 0;
              collection.filter(function(record) {
                return record.c == c_values[1];
              }, function(error, record) {
                if (error) {
                  throw error;
                }
                if (record === null) {
                  assert.equal(result_count, OBJECT_COUNT / 3);
                  collection.end(function(err) {
                    if (err) {
                      throw err;
                    }
                    next();
                  });
                } else {
                  assert.equal(record.c, c_values[1]);
                  result_count ++;
                }

              });
            }
          });
        })(i);
      }
    });
  });
};