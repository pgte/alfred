var exec = require('child_process').exec;
var COLL_PATH = __dirname + '/../../tmp/collection_recovery_test.alf';

module.exports.setup = function(next) {
  var command = 'cp -f ' + __dirname + '/recovery_test_sample.alf ' + COLL_PATH;
  exec(command, function(err, stdout, stderr) {
    if (err) {
      next(err);
    }
    
    next();
  });
  
};

module.exports.run = function(next) {
  
  var assert = require('assert');
  var fs = require('fs');
  var Collection = require(__dirname + '/../../lib/alfred/collection.js');
  Collection.open(COLL_PATH, function(err, collection) {
    if (err) { next(err); return; }
    
    var original_record_count = 0;
    
    collection.read(function(err, record) {
      if (err) { next(err); return; }
      if (record) {
        original_record_count ++;
      } else {
        // ended
        // close collection
        collection.end(function(err) {
          if (err) { next(err); return; }

          // Now, let's plant some rotten eggs

          fs.stat(COLL_PATH, function(err, stat) {
            if (err) { next(err); return; }
            var size = stat.size;
            fs.open(COLL_PATH, 'a', 0600, function(err, fd) {
              if (err) { next(err); return; }
              fs.write(fd, new Buffer(" "), 0, 1, size - 1, function(err, written) {
                if (err) { next(err); return; }
                assert.equal(1, written);
                fs.write(fd, new Buffer(" "), 0, 1, 0, function(err, written) {
                  if (err) { next(err); return; }
                  assert.equal(1, written);
                  fs.close(fd);

                  Collection.open(COLL_PATH, function(err, collection) {
                    if (err) { next(err); return; }

                    var spoiled_record_count = 0;

                    collection.read(function(err, record) {
                      if (err) { next(err); return; }
                      if (record) {
                        spoiled_record_count ++;
                      } else {
                        assert.equal(spoiled_record_count, 998);
                        next();
                      }
                    }, true);
                  });
                });
              });
            });
          });
        });
      }
    }, true);
  });
};