var LogStream   = require('../log_stream');

module.exports = function(master, stream) {
  try {
    var logStream = LogStream.open(master.logger);
    logStream.on('error', function(err) {
      stream.write(JSON.stringify({"error": err}));
    });
    util.pump(logStream, stream, function() {
      logStream.destroy();
    });
  } catch (exception) {
    stream.write(JSON.stringify({"error": err}));
  }
};