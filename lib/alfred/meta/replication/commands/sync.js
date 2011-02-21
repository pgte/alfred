var LogStream   = require('../log_stream');

var pump = function(readStream, writeStream, callback) {
  
    var callbackCalled = false;

    function call(a, b, c) {
      if (callback && !callbackCalled) {
        callback(a, b, c);
        callbackCalled = true;
      }
    }

    if (!readStream.pause) {
      readStream.pause = function() {readStream.emit('pause');};
    }

    if (!readStream.resume) {
      readStream.resume = function() {readStream.emit('resume');};
    }

    readStream.addListener('data', function(chunk) {
      try {
        if (writeStream.write(chunk) === false) { readStream.pause(); }
      } catch (err) {
        try { readStream.destroy(); } catch(excp) { /* do nothing */ }
        try { writeStream.emd(); } catch(excp2) { /* do nothing */ }
        call(err);
      }
      
    });

    writeStream.addListener('pause', function() {
      readStream.pause();
    });

    writeStream.addListener('drain', function() {
      readStream.resume();
    });

    writeStream.addListener('resume', function() {
      readStream.resume();
    });

    readStream.addListener('end', function() {
      writeStream.end();
    });

    readStream.addListener('close', function() {
      call();
    });

    readStream.addListener('error', function(err) {
      writeStream.end();
      call(err);
    });

    writeStream.addListener('error', function(err) {
      readStream.destroy();
      call(err);
    });
};

module.exports = function(master, args, stream) {
  try {
    var logStream = LogStream.open(master.logger, args && args.from);
    logStream.on('error', function(err) {
      stream.write(JSON.stringify({"error": err}));
    });
    pump(logStream, stream, function(err) {
      logStream.destroy();
    });
  } catch (exception) {
    stream.write(JSON.stringify({"error": err}));
  }
};