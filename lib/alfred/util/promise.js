module.exports = function(action, error_callback) {
  
  var done = false, cb;
  var queue = [];
  
  action(function(err) {
    if (err) { error_callback(err); return; }
    done = true;
    while (queue.length > 0) {
      cb = queue.splice(0, 1)[0];
      cb();
    }
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