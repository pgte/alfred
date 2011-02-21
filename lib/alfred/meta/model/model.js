var registrar = require('./registrar'),
    promise   = require('../../util/promise'),
    Promises   = require('../../util/promises'),
    definers  = require('./schema').definers,
    Finder    = require('../finder'),
    mixin     = require('../../util/mixin'),
    Document  = require('./document'),
    util      = require('util'),
    EventEmitter   = require('events').EventEmitter;

var Model = function(database, name, options) {
  var self = this;
  this.database = database;
  this.name = name;
  this.schema = {};
  
  this.protoBase = {};
  this.protoBase.__proto__ = Document.proto;
  
  registrar.put(name, this);
  if (!this.database) {
    throw new Error('No database connection defined');
  }
  
  this.collPromise = promise(function(cb) {
    database.ensure(self.name, options, function(err, coll) {
      if (err) { cb(err); return; }
      self.collection = coll;
      cb(null);
    });
  }, function(err) {
    self.database.emit('error', err);
  });
  this.promises = Promises.new();
  this.promises.add(this.collPromise);
  
  if (self.schema.indexes && self.schema.indexes.length > 0) {
    this.collPromise(function() {
      self.schema.indexes.forEach(function(indexDef) {
        self.promises.add(promise(function(cb) {
          self.collection.ensureIndex(indexDef.name, indexDef.options || {ordered: true}, indexDef.fn, function(err) {
            if (err) { cb(err); return; }
            if (-- indexesLeft === 0) {
              cb(null);
            }
          });
        }, function(err) {
          self.database.emit('error', err);
        }));
      });
    });
  }
  
  this.promises.done(function() {
    self._promiseDone();
  });

};

util.inherits(Model, EventEmitter);

module.exports.define = function(database, name, options) {
  return new Model(database, name, options);
};

Model.prototype._error = function(err) {
  this.database.emit('error', err);
};

Model.prototype._swap = function(fa, fb) {
  var f = this[fb];
  this[fb] = this[fa];
  this["_waiting_" + fb] = f;
};

Model.prototype._promiseDone = function() {
  if (!this._promiseReallyDone) {
    this._promiseReallyDone = true;
    this._swap('_get', 'get');
    this._swap('_put', 'put');
    this._swap('_atomic', 'atomic');
    this._swap('_delete', 'delete');
  }
};

Model.prototype._promiseUndone = function() {
  if (this._promiseReallyDone) {
    delete this._promiseReallyDone;
    this.get = this._waiting_get;
    this.put = this._waiting_put;
    this.atomic = this._waiting_atomic;
    this.delete = this._waiting_delete;
  }
};

Model.prototype._get = function(id, callback) {
  var self = this;
  self.collection.get(id, function(err, value) {
    if (err) { self._error(err); return; }
    if (value === null) {
      callback(null); return;
    }
    var doc = self.instantiate(value);
    doc.id = id;
    doc.doc.id = id;
    callback(doc);
  });
};

Model.prototype.get = function(id, callback) {
  var self = this;
  this.promises.done(function() {
    self._get(id, callback);
  });
};

Model.prototype._put = function(id, value, callback) {
  var self = this;
  delete value.id;
  self.collection.put(id, value, function(err) {
    if (err) { self._error(err); return; }
    callback();
  });
};

Model.prototype.put = function(id, value, callback) {
  var self = this;
  this.promises.done(function() {
    self._put(id, value, callback);
  });
};

Model.prototype._atomic = function(id, doc_callback, final_callback) {
  var self = this;
  self.collection.atomic(id, function(err, value) {
    if (err) { self._error(err); return; }
    var doc = self.instantiate(value);
    var retDoc = doc_callback(doc);
    if (retDoc.isValid()) {
      return retDoc.doc;
    } else {
      final_callback(retDoc.errors);
      return null;
    }
  }, function(err) {
    if (err) { self._error(err); return; }
    final_callback();
  });
};

Model.prototype.atomic = function(id, doc_callback, final_callback) {
  var self = this;
  this.promises.done(function() {
    self._atomic(id, doc_callback, final_callback);
  });
};

Model.prototype._delete = function(id, callback) {
  this.collection.destroy(id, function(err) {
    if (err) { self._error(err); return; }
    callback();
  });
};

Model.prototype.delete = function(id, callback) {
  var self = this;
  this.promises.done(function() {
    self.delete(id, callback);
  });
};

Model.prototype.find = function(query) {
  var self = this;
  var finder = Finder.create(null, query);
  var old_execute = finder.execute,
      old_stream  = finder.stream;
      old_all     = finder.all;
      
  finder.execute = function(callback) {
    self.promises.done(function() {
      finder.setKeyMap(self.collection);
      old_execute.call(finder, function(err, key, value) {
        if (err) { self._error(err); return; }
        var doc = self.instantiate(value);
        doc.id = key;
        callback(doc);
      });
    });
    return finder;
  };

  finder.all = function(callback) {
    self.promises.done(function() {
      finder.setKeyMap(self.collection);
      old_all.call(finder, function(err, records) {
        if (err) { self._error(err); return; }
        var docs = [];
        records.forEach(function(record) {
          var doc = self.instantiate(record.value);
          doc.id = record.key;
          docs.push(doc);
        });
        callback(docs);
      });
    });
    return finder;
  };

  finder.stream = function(callback) {
    self.promises.done(function() {
      finder.setKeyMap(self.collection);
      old_stream.call(finder, function(stream) {
        
        stream.on('error', function(err) {
          self._error(err);
        });

        stream.on('record', function(key, record) {
          value.id = key;
          var doc = self.instantiate(record.value);
          doc.id = record.key;
          stream.emit('document', doc);
        });
        
        return stream;
      });
    });
    return finder;
  };
  return finder;
};

Model.prototype.instantiate = function(doc) {
  docObj = Document.create(this, doc || {});
  docObj.__proto__ = this.protoBase;
  return docObj;
};

Model.prototype.new = Model.prototype.instantiate;

Model.prototype.property = function(name, typeOrSchema, schema) {
  var definer = {};
  var type = (function () {
    switch (typeof(typeOrSchema)) {
      case "string":    return typeOrSchema;
      case "function":  return typeOrSchema.name.toLowerCase();
      case "object":    schema = typeOrSchema; break;
      case "undefined": return "string";
      default:          throw new(Error)("Argument Error"); 
    }
  })();
  schema = schema || {};
  schema.type = schema.type || type;
  if (!this.schema.properties) {
    this.schema.properties = {};
  }

  this.schema.properties[name] = definer.property = schema;

  mixin(definer, definers.all, definers[schema.type] || {});
  
  this.protoBase.__defineGetter__(name, function() {
    return this.doc[name];
  });
  this.protoBase.__defineSetter__(name, function(value) {
    this.doc[name] = value;
  });

  return definer;
};

Model.prototype.index = function(name, fn, options) {
  var self = this;
  
  self._promiseUndone();
  self.promises.done(function() {
    self.promises.add(promise(function(cb) {
      self.collection.ensureIndex(name, options || {ordered: true}, fn, cb);
    }, function(err) {
      self.database.emit('error', err);
    }));
  });
};