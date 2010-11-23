module.exports.run = function(benchmark, next) {

  var assert = require('assert');
  
  require(__dirname + '/../lib/alfred/key_map.js').open(__dirname + '/../tmp/key_map_benchmark.alf', function(err, key_map) {
    if (err) {
      throw err;
    }

    var KEY_COUNT = 50000;
    var RETRIEVE_RANDOM_COUNT = 10;

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

    var compareObjects = function(a, b) {
      return a.a === b.a && a.b === b.b && a.c === b.c;
    };

    var keys = [];
    var map = {};

    key_map.clear(function(err) {
      if (err) {
        throw err;
      }
      var written = 0;
      benchmark.start('Insert object into key map', KEY_COUNT);
      for (var i = 0; i < KEY_COUNT; i ++) {
        var value = createRandomObject();
        var key = createRandomString(16);
        keys.push(key);
        map[key] = value;
        key_map.put(key, value, function(err) {
          if (err) {
            throw err;
          }
          written ++;
          if (written == KEY_COUNT) {
            // wait for flush
            setTimeout(function() {

              var sample_key_index = Math.floor(Math.random() * KEY_COUNT)
              var key = keys[sample_key_index];
              var retrieved_keys = 0;

              benchmark.start('Retrieve a random object by key from a ' + KEY_COUNT + ' hash map', RETRIEVE_RANDOM_COUNT);
              for(var i = 0; i < RETRIEVE_RANDOM_COUNT; i++) {
                key_map.get(key, function(err, record) {
                  if (err) {
                    throw err;
                  }
                  retrieved_keys ++;
                  if (retrieved_keys == RETRIEVE_RANDOM_COUNT) {
                    benchmark.end();
                    next();
                  }
                });        
              }

              // test than when we try to retrieve with a non-existing ket, we get null back

            }, 1000);
          }
        });
      }
      benchmark.end();

    });
    
  });
  

}

