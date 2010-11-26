module.exports.run = function(next) {
  var assert = require('assert');
  
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_test.alf', function(err, collection) {

    if (err) {
      throw err;
    }

    var OBJECT_COUNT = 15000;

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

    var createRandomObject = function(c_value) {
      return {
        a: createRandomString(random(10)),
        b: createRandomString(random(100)),
        c: c_value
      };
    };

    var c_values = [createRandomString(random(100)), createRandomString(random(100)), createRandomString(random(100))]

    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      for (var i = 0; i < OBJECT_COUNT; i ++) {
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
      }


    });
    
  });
  

}

