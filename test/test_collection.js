module.exports.run = function(next) {
  
  var assert = require('assert');
  var random = require('../tools/random_generator');
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_test.alf', function(err, collection) {
    if (err) {
      throw err;
    }
    
    var records = [];

    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      for (var i = 0; i < 1000; i ++) {
        var record = random.createRandomObject();
        records.push(record);
        collection.write(record, function(err) {
          if (err) {
            throw err;
          }
          written ++;
          if (written == 1000) {
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
          }
        });
      }

    });
    
  });
  

}

