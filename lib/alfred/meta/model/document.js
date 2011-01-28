var validator = require('./validator'),
    util      = require('util');

var Document = function(model, doc) {
  this.model = model;
  this.doc = doc;
};

module.exports.create = function(model, doc) {
  return new Document(model, doc);
};

Document.prototype.isNew = function() {
  return !this.id;
};

Document.prototype._assureNotNew = function() {
  if (this.isNew()) { throw new Error('record is not saved yet'); }
}

var newId = function() {
  function S4() {
     return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
  }
  return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
};

Document.prototype.save = function(callback) {
  var self = this;
  if (self.isNew()) {
    self.id = newId();
    self.doc.id = self.id;
  }
  var validation = validator.validate(self.doc, self.model.schema);
  if (!validation.valid) {
    callback(validation.errors);
    return;
  }
  self.model.put(self.id, self.doc, function() {
    callback(null);
  });
};

Document.prototype.atomic = function(callback, document_callback, saved_callback) {
  var self = this;
  self._assureNotNew();
  self.model.atomic(self.id, document_callback, saved_callback);
};

Document.prototype.destroy = function(callback) {
  this._assureNotNew();
  this.model.delete(this.id, callback)
};

Document.prototype.reload = function(callback) {
  var self = this;
  self._assureNotNew();
  self.model.get(self.id, function(record) {
    this.doc = record;
    this.doc.id = self.id;
    callback();
  });
};

Document.prototype.toString = function() {
  return util.inspect(this.doc);
};

Document.prototype.equal = function(obj) {
  if (!obj) {
    console.log('B');
    return false;
  }
  
  if (this.model != obj.model) {
    console.log('C');
    return false;
  }
  
  var pSlice = Array.prototype.slice;
  
  return (function _deepEqual(a, b) {
    
    if (a === b) {
      return true;
    }
    // an identical 'prototype' property.
    if (a.prototype !== b.prototype) {console.log('D'); return false;}
    //~~~I've managed to break Object.keys through screwy arguments passing.
    //   Converting to array solves the problem.
    try {
      var ka = Object.keys(a),
          kb = Object.keys(b),
          key, i;
    } catch (e) {//happens when one is a string literal and the other isn't
      console.log(e)
      console.log('E');
      return false;
    }
    // having the same number of owned properties (keys incorporates
    // hasOwnProperty)
    if (ka.length != kb.length) {
      console.log('F');
      return false;
    }
      
    //the same set of keys (although not necessarily the same order),
    ka.sort();
    kb.sort();
    //~~~cheap key test
    for (i = ka.length - 1; i >= 0; i--) {
      if (ka[i] != kb[i]) {
        console.log('G');
        return false;
      }
    }
    //equivalent values for every corresponding key, and
    //~~~possibly expensive deep test
    for (i = ka.length - 1; i >= 0; i--) {
      key = ka[i];
      console.log(key);
      if (!_deepEqual(a[key], b[key])) {console.log('A'); return false;}
    }
    return true;
  })(this.doc, obj.doc);
};

module.exports.proto = Document.prototype;