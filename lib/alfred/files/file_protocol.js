var AlfredCheckError = require('./alfred_check_error'),
    File             = require('./file'),
    StringDecoder    = require('string_decoder').StringDecoder;

var MAGIC_CHAR_1 = 0xDC; 
var MAGIC_CHAR_2 = 0x80;

var FileProtocol = function(pos, recordCallback, warnCallback) {
  this.pos = pos;
  this.startRecordPos = 0;
  this.recordCallback = recordCallback;
  this.warnCallback = warnCallback;
  this.reset();
};

module.exports.create = function(pos, recordCallback, warnCallback) {
  return new FileProtocol(pos, recordCallback, warnCallback);
};

FileProtocol.prototype.reset = function() {
  this.state = 'idle';
  this.lengthString = '';
  this.buffers = [];
  this.trailerString = '';
};

FileProtocol.prototype.validateTrailerAndCallback = function() {
  var decoder = new StringDecoder('utf8'),
      record = '',
      length = 54 + this.recordLength,
      trailer;
      
  this.buffers.forEach(function(buffer) {
    record += decoder.write(buffer);
  });
  trailer = File.calculateTrailer(record, this.startRecordPos, length);
  if (trailer != this.trailerString) {
    throw new AlfredCheckError('at pos ' + this.pos + ': trailer does not match. Should be ' + trailer + ' and is ' + this.trailerString + '. record length was ' + this.recordLength);
  }
  this.recordCallback(record, this.startRecordPos, length);
};

FileProtocol.prototype.magicChar1 = function() {
  if (this.state != 'idle') {
    throw new AlfredCheckError('at pos ' + this.pos + ': magic char 1 should not come in the middle of a record', 'MAGIC_CHAR_1_MIDDLE_OF_RECORD');
  }
  this.state = 'magic char 1';
};

FileProtocol.prototype.magicChar2 = function() {
  if (this.state != 'magic char 1') {
    throw new AlfredCheckError('at pos ' + this.pos + ': magic char 2 should come only after magic char 1', 'INVALID_MAGIC_CHAR_2');
  }
  this.state = 'magic char 2';
};

FileProtocol.prototype.length = function(length) {
  if (this.state != 'magic char 2' && this.state != 'in length') {
    throw new AlfredCheckError('at pos ' + this.pos + ': length should come after magic char 2. in state ' + this.state, 'INVALID_LENGTH_POS');
  }
  this.lengthString += length;
  if (this.lengthString.length < 12) {
    this.state = 'in length';
  } else {
    this.remainingLength = this.recordLength = parseInt(this.lengthString, 10);
    this.state = 'finished length';
  }
};

FileProtocol.prototype.record = function(buffer) {
  this.buffers.push(buffer);
};

FileProtocol.prototype.trailer = function(trailer) {
  this.trailerString += trailer;
  if (this.trailerString.length > 40) {
    throw new Error('at pos ' + this.pos + ': parser error: trailer is ' + this.trailerString.length + ' bytes');
  }
  if (this.trailerString.length === 40) {
    this.state = 'finished';
    this.validateTrailerAndCallback();
    this.startRecordPos += (54 + this.recordLength);
    this.reset();
  }
};

FileProtocol.prototype.write = function(buffer) {
  var byt, len, piece, advanceBy, i;
  
  i = 0;
  while (i < buffer.length) {
    advanceBy = 1;
    try {
      byt = buffer[i];
      if (this.state == 'idle' && byt === MAGIC_CHAR_1) {
        this.magicChar1();
      } else if (this.state == 'magic char 1' && byt === MAGIC_CHAR_2) {
        this.magicChar2();
      } else {
        if (this.state == 'magic char 2' || this.state == 'in length') {
          len = Math.min(12 - this.lengthString.length, buffer.length - i);
          advanceBy = len;
          piece = buffer.slice(i, i + len);
          this.length(piece.toString('utf8'));
        } else if (this.state == 'finished length' || this.state == 'in record') {
          this.state = 'in record';
          len = Math.min(this.remainingLength, buffer.length - i);
          advanceBy = len;
          piece = buffer.slice(i, i + len);
          this.record(piece);
          this.remainingLength -= len;
          if (this.remainingLength === 0) {
            this.state = 'finished record';
          }
        } else if (this.state == 'finished record' || this.state == 'in trailer') {
          this.state = 'in trailer';
          len = Math.min(40 - this.trailerString.length, buffer.length - i);
          advanceBy = len;
          this.trailer(buffer.slice(i, i + len).toString('utf8'));
        } else {
          throw new AlfredCheckError('invalid sequence in state ' + this.state + ' on pos ' + this.pos);
        }
      }
    } catch (err) {
      if (!(err instanceof AlfredCheckError)) {
        throw err;
      } else {
        advanceBy = 1;
        this.startRecordPos += 1;
        // log and skip to the next byte
        //console.log("check error: " + err.message);
        //console.log('advancing by ' + advanceBy);
        this.warnCallback(err);
        this.reset();
      }
    }
    //console.log(advancing to)
    this.pos += advanceBy;
    i += (advanceBy || 1);
  }
};