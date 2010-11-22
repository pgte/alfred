module.exports.run = function(benchmark, next) {
  
  var collection = require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_benchmark.alf');
  var assert = require('assert');
  
  var OBJECT_COUNT = 100000;
  
  collection.on('error', function(error) {
    throw error;
  });

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
      b: createRandomString(50),
      c: c_value
    };
  };

  var compareObjects = function(a, b) {
    return a.a === b.a && a.b === b.b && a.c === b.c;
  };

  var c_values = [createRandomString(50), createRandomString(50), createRandomString(50)]
  
  collection.prune(function() {
    for (var i = 0; i < OBJECT_COUNT; i ++) {
      record = createRandomObject(c_values[i % c_values.length]);
      collection.write(record);
    }
    
    // wait for flush
    setTimeout(function() {
      var index = 0;
      benchmark.start('Collection filter', OBJECT_COUNT);
      collection.filter(function(record) {
        return record.c == c_values[1];
      }, function(error, result) {
        if (error) {
          throw error;
        }
        benchmark.end();
        next();
      });
    }, 2000);
  
  });

}

