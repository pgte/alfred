module.exports.run = function(benchmark, next) {
  
  var OBJECT_COUNT= 1000;
  
  benchmark.start('opening collection');
  require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_bench.alf', function(err, collection) {
    if (err) {
      throw err;
    }
    
    benchmark.end();
    var assert = require('assert');

    var createRandomString = function(string_length) {
      var chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz";
    	var randomstring = '';
    	for (var i=0; i<string_length; i++) {
    		var rnum = Math.floor(Math.random() * chars.length);
    		randomstring += chars.substring(rnum,rnum+1);
    	}
    	return randomstring;
    };

    var createRandomObject = function() {
      return {
        a: createRandomString(10),
        b: createRandomString(50),
        c: createRandomString(50)
      };
    };

    benchmark.start('prunning collection');
    collection.clear(function(err) {
      if (err) {
        throw err;
      }
      benchmark.end();
      benchmark.start('populating collection', OBJECT_COUNT);
      for (var i = 0; i < OBJECT_COUNT; i ++) {
        record = createRandomObject();
        collection.write(record, function(err) {
          if (err) {
            throw err;
          }
        });
      }
      benchmark.end();

      // wait for flush
      setTimeout(function() {
        var index = 0;
        benchmark.start('reading entire collection of ' + OBJECT_COUNT + ' records');
        collection.read(function(error, record) {
          assert.equal(error, null);
          if (record === null) {
            benchmark.end();
            collection.end();
            next();
          } else {
            index ++;
          }
        }, true);
      }, 2000);
      
    });
  });
}

