var util = require('util'),
    Collection = require(__dirname + '/../lib/alfred/collection.js');

var col_path = __dirname + '/../tmp/collection_test.alf';

module.exports.run = function(next) {
  
  var assert = require('assert');
  var random = require('../tools/random_generator');
  Collection.open(col_path, {read_buffer_size: 1024}, function(err, collection) {
    if (err) { next(err); return; }
    
    var records = [];

    collection.clear(function(err) {
      if (err) { next(err); return; }
      var written = 0;
      for (var i = 0; i < 1000; i ++) {
        (function(i) {
          var record = random.createRandomObject();
          records.push(record);
          collection.write(record, function(err) {
            if (err) { next(err); return; }
            written ++;
            if (written == 1000) {
              var index = 0;
              collection.end(function(err) {
                if (err) { next(err); return; }
                Collection.open(col_path, function(err, collection) {
                  if (err) { next(err); return; }
                  collection.read(function(err, record) {
                    if (err) { next(err); return; }
                    if(record === null) {
                      // reached the end
                      assert.equal(records.length, index);
                      collection.end(function(err) {
                        if (err) { next(err); return; }
                        next();
                      });

                    } else {
                      assert.deepEqual(record, records[index], "Object at index " + index + ' differs: original: ' + util.inspect(records[index]) + ', read: ' + util.inspect(record));
                      index ++;
                    }
                  }, true);
                })
              });
            }
          });
        })(i);
      }
    });
  });
};