module.exports.run = function(next) {
  var assert = require('assert');
  
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_test.alf', function(err, collection) {

    if (err) {
      throw err;
    }

    var OBJECT_COUNT = 15000;

    var createRandomString = function(string_length) {
      var chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz";
      var randomstring = '';
      for (var i=0; i<string_length; i++) {
        var rnum = Math.floor(Math.random() * chars.length);
        randomstring += chars.substring(rnum,rnum+1);
      }
      return randomstring;
    };

    var createRandomObject = function(c_value) {
      return {
        a: createRandomString(10),
        b: createRandomString(100),
        c: c_value
      };
    };

    var c_values = [createRandomString(100), createRandomString(100), createRandomString(100)]

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
            // wait for flush
            setTimeout(function() {
              var result_count = 0;
              collection.filter(function(record) {
                return record.c == c_values[1];
              }, function(error, record) {
                if (error) {
                  throw error;
                }
                if (record === null) {
                  assert.equal(result_count, OBJECT_COUNT / 3);
                  next();
                } else {
                  assert.equal(record.c, c_values[1]);
                  result_count ++;
                }

              });
            }, 1000);
          }
        });
      }


    });
    
  });
  

}

