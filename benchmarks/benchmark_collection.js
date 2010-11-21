module.exports.run = function(benchmark, next) {
  
  var OBJECT_COUNT= 10000;
  
  benchmark.start('opening collection');
  var collection = require(__dirname + '/../lib/alfred/collection.js').open(__dirname + '/../tmp/collection_test.alf');
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
      b: createRandomString(100),
      c: createRandomString(100)
    };
  };

  benchmark.start('prunning collection');
  collection.prune(function() {
    benchmark.end();
    benchmark.start('populating collection', OBJECT_COUNT);
    for (var i = 0; i < OBJECT_COUNT; i ++) {
      record = createRandomObject();
      collection.write(record);
    }
    benchmark.end();
  
    // wait for flush
    setTimeout(function() {
      var index = 0;
      benchmark.start('reading collection', OBJECT_COUNT);
      collection.read(function(error, record) {
        assert.equal(error, null);
        index ++;
        if(index == OBJECT_COUNT) {
          // loaded all
          benchmark.end();
          collection.end();
          next();
        }
      });
    }, 2000);
  
  });

}

