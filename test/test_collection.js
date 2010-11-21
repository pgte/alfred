module.exports.run = function(next) {
  
  var collection = require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_test.alf');
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
      b: createRandomString(100),
      c: createRandomString(100)
    };
  };

  var compareObjects = function(a, b) {
    return a.a === b.a && a.b === b.b && a.c === b.c;
  };

  var records = [];

  collection.prune(function() {
    for (var i = 0; i < 1000; i ++) {
      record = createRandomObject()
      records.push(record);
      collection.write(record);
    }
  
    // wait for flush
    setTimeout(function() {
      var index = 0;
      collection.read(function(error, record) {
        assert.equal(error, null);
        assert.ok(compareObjects(record, records[index], "Object at index " + index + ' differs.'));
        index ++;
        if(index == records.length) {
          // loaded all
          collection.end();
          next();
        }
      });
    }, 2000);
  
  });

}

