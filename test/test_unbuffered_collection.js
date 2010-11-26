module.exports.run = function(next) {
  
  var assert = require('assert');
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/buffered_collection_test.alf', function(err, collection) {
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

    var createRandomObject = function() {
      return {
        a: createRandomString(random(10)),
        b: createRandomString(random(100)),
        c: createRandomString(random(100))
      };
    };

    var records = [];

    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      for (var i = 0; i < 10000; i ++) {
        var record = createRandomObject();
        records.push(record);
        collection.write(record, function(err) {
          if (err) {
            throw err;
          }
          written ++;
          if (written == 1000) {
            // wait for flush
            setTimeout(function() {
              var index = 0;
              collection.read(function(error, record) {
                assert.equal(error, null);
                if(record === null) {
                  // reached the end
                  assert.equal(records.length, index);
                  collection.end(function(err) {
                    if (err) {
                      throw err;
                    }
                    next();
                  });
                  
                } else {
                  assert.deepEqual(record, records[index], "Object at index " + index + ' differs.');
                  index ++;
                }
              }, true);
            }, 2000);
          }
        });
      }

    });
    
  }, {buffered: false});

}

