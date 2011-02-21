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
};

var newId = function() {
  return (Math.floor(Math.random() * 100000000000000000) + Date.now()).toString(32);
};

Document.prototype.isValid = function() {
  var validation = validator.validate(this.doc, this.model.schema);
  if (validation.valid) {
    return true;
  } else {
    this.errors = validation.errors;
    return false;
  }
};

Document.prototype.save = function(callback) {
  var self = this;
  if (self.isNew()) {
    self.id = newId();
    self.doc.id = self.id;
  }
  var validation = self.validate();
  if (!validation.valid) {
    callback(validation.errors);
    return;
  }
  self.model.emit('beforeSave', self);
  self.model.put(self.id, self.doc, function() {
    callback(null);
    self.model.emit('afterSave', self);
  });
};

Document.prototype.validate = function() {
  var validation;
  this.model.emit('beforeValidate', this);
  validation = validator.validate(this.doc, this.model.schema);
  this.model.emit('afterValidate', this);
  return validation;
};

Document.prototype.atomic = function(document_callback, saved_callback) {
  var self = this;
  self._assureNotNew();
  self.model.atomic(self.id, document_callback, saved_callback);
};

Document.prototype.destroy = function(callback) {
  this._assureNotNew();
  this.model.delete(this.id, callback);
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

Document.prototype.inspect = function() {
  return util.inspect(this.doc);
};

Document.prototype.toString = Document.prototype.inspect;

Document.prototype.equal = function(obj) {
  if (!obj) {
    return false;
  }
  
  if (this.model != obj.model) {
    return false;
  }
  
  var pSlice = Array.prototype.slice;
  
  return (function _deepEqual(a, b) {
    
    if (a === b) {
      return true;
    }
    // an identical 'prototype' property.
    if (a.prototype !== b.prototype) {return false;}
    //~~~I've managed to break Object.keys through screwy arguments passing.
    //   Converting to array solves the problem.
    var ka, kb, key, i;
    try {
      ka = Object.keys(a);
      kb = Object.keys(b);
    } catch (e) {//happens when one is a string literal and the other isn't
      return false;
    }
    // having the same number of owned properties (keys incorporates
    // hasOwnProperty)
    if (ka.length != kb.length) {
      return false;
    }
      
    //the same set of keys (although not necessarily the same order),
    ka.sort();
    kb.sort();
    //~~~cheap key test
    for (i = ka.length - 1; i >= 0; i--) {
      if (ka[i] != kb[i]) {
        return false;
      }
    }
    //equivalent values for every corresponding key, and
    //~~~possibly expensive deep test
    for (i = ka.length - 1; i >= 0; i--) {
      key = ka[i];
      if (!_deepEqual(a[key], b[key])) { return false; }
    }
    return true;
  })(this.doc, obj.doc);
};

module.exports.proto = Document.prototype;