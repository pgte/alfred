module.exports = function(action, error_callback) {
  
  var done = false;
  var queue = [];
  
  action(function(err) {
    if (err) { error_callback(err); return; }
    done = true;
    queue.forEach(function(cb) {
      cb();
    });
    delete queue;
  });

  return function(cb) {
    if (done) {
      cb();
    } else {
      queue.push(cb);
    }
  };

};