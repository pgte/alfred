module.exports.run = function(next) {
  
  var collection = require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_filter_test.alf');
  var assert = require('assert');
  
  var OBJECT_COUNT = 900;
  
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
      b: createRandomString(100),
      c: c_value
    };
  };

  var compareObjects = function(a, b) {
    return a.a === b.a && a.b === b.b && a.c === b.c;
  };

  var c_values = [createRandomString(100), createRandomString(100), createRandomString(100)]
  
  collection.prune(function() {
    for (var i = 0; i < OBJECT_COUNT; i ++) {
      record = createRandomObject(c_values[i % c_values.length]);
      collection.write(record);
    }
    
    // wait for flush
    setTimeout(function() {
      var index = 0;
      collection.filter(function(record) {
        return record.c == c_values[1];
      }, function(error, result) {
        if (error) {
          throw error;
        }
        assert.equal(result.length, OBJECT_COUNT / 3);
      });
    }, 2000);
  
  });

}

