module.exports.run = function(next) {
  
  var assert = require('assert');
  var random = require('../tools/random_generator');

  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/unbuffered_collection_test.alf', {buffered: false}, function(err, collection) {
    if (err) { next(err); return; }
    
    var records = [];

    collection.clear(function(err) {
      if (err) { next(err); return; }
      var written = 0;
      for (var i = 0; i < 10000; i ++) {
        (function(i) {
          var record = random.createRandomObject();
          records.push(record);
          collection.write(record, function(err) {
            if (err) { next(err); return; }
            written ++;
            if (written == 10000) {
              var read = 0;
              collection.read(function(error, record) {
                if (err) { next(err); return; }
                if(!record) {
                  // reached the end
                  assert.equal(records.length, read);
                  collection.end(function(err) {
                    if (err) { next(err); return; }
                    next();
                  });

                } else {
                  assert.deepEqual(record, records[read], "Object at index " + read + ' differs.');
                  read ++;
                  //console.log(read);
                }
              }, true);
            }
          });
        })(i);
      }
    });
  });
};