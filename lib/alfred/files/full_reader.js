var EventEmitter   = require('events').EventEmitter,
    util           = require('util'),
    fs             = require('fs'),
    FileProtocol   = require('./file_protocol');

var MAGIC_CHAR_1 = 0xDC; 
var MAGIC_CHAR_2 = 0x80;

var FullReader = function(file_path) {
  var self = this;
  this.file_path = file_path;
  self.read();
};

util.inherits(FullReader, EventEmitter);

module.exports.open = function(filePath) {
  return new FullReader(filePath);
};

FullReader.prototype.read = function() {
  var self = this;
  var rs = fs.createReadStream(this.file_path),
      protocol = FileProtocol.create(0, function(record, pos, length) {
        self.emit('data', record, pos, length);
      }, function(warn) {
        self.emit('warn', warn);
      });
  
  rs.on('data', function(data) {
    protocol.write(data);
  });
  
  rs.on('error', function(err) {
    self.emit('error', err);
  });
  
  rs.on('end', function() {
    self.emit('end', protocol.pos);
  });
  
};