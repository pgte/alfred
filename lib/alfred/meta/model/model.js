var registrar = require('./registrar'),
    promise   = require('../../util/promise'),
    definers  = require('./schema').definers,
    Finder    = require('../finder'),
    mixin     = require('../../util/mixin'),
    Document  = require('./document');

var Model = function(database, name, schema, options) {
  var self = this;
  this.database = database;
  this.name = name;
  this.schema = schema || {};
  
  this.protoBase = {};
  this.protoBase.__proto__ = Document.proto;
  
  registrar.put(name, this);
  if (!this.database) {
    throw new Error('No database connection defined');
  }
  
  this.initiatedPromise = promise(function(callback) {
    database.ensure(self.name, options, function(err, coll) {
      if (err) { callback(err); return; }
      self.collection = coll;
      callback(null);
    });
  }, function(err) {
    self.database.emit('error', err);
  });
};

module.exports.define = function(database, name, schema, options) {
  return new Model(database, name, schema);
};

Model.prototype._error = function(err) {
  this.database.emit('error', err);
};

Model.prototype.get = function(id, callback) {
  var self = this;
  this.initiatedPromise(function() {
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
  });
};

Model.prototype.put = function(id, value, callback) {
  var self = this;
  this.initiatedPromise(function() {
    delete value.id;
    self.collection.put(id, value, function(err) {
      if (err) { self._error(err); return; }
      callback();
    })
  });
};

Model.prototype.atomic = function(id, doc_callback, final_callback) {
  var self = this;
  this.initiatedPromise(function() {
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
  });
  
};

Model.prototype.delete = function(id, callback) {
  var self = this;
  this.initiatedPromise(function() {
    self.collection.destroy(id, function(err) {
      if (err) { self._error(err); return; }
      callback();
    });
  });
};

Model.prototype.find = function(query) {
  var self = this;
  var finder = Finder.create(null, query);
  var old_execute = finder.execute,
      old_stream  = finder.stream;
      old_all     = finder.all;
      
  finder.execute = function(callback) {
    this.initiatedPromise(function() {
      finder.key_map = self.collection;
      old_execute.call(finder, function(err, key, value) {
        if (err) { self._error(err); return; }
        var doc = self.instantiate(value);
        doc.id = key;
        callback(doc);
      });
    });
  };

  finder.all = function(callback) {
    this.initiatedPromise(function() {
      finder.key_map = self.collection;
      old_all.call(finder, function(err, records) {
        if (err) { self._error(err); return; }
        var docs = [];
        records.forEach(function(record) {
          docs.push(self.instantiate(record));
        });
        callback(docs);
      });
    });
  };

  finder.stream = function(callback) {
    this.initiatedPromise(function() {
      finder.key_map = self.collection;
      old_stream.call(finder, function(stream) {
        
        stream.on('error', function(err) {
          self._error(err);
        });

        stream.on('record', function(key, value) {
          value.id = key;
          stream.emit('document', self.instantiate(value));
        });
        
        return stream;
      });
    });
  };
  return finder;
};

Model.prototype.instantiate = function(doc) {
  var doc = Document.create(this, doc || {});
  doc.__proto__ = this.protoBase;
  return doc;
};

Model.prototype.new = Model.prototype.instantiate;

Model.prototype.property = function(name, typeOrSchema, schema) {
  var definer = {};
  var type = (function () {
    switch (typeof(typeOrSchema)) {
      case "string":    return typeOrSchema;
      case "function":  return typeOrSchema.name.toLowerCase();
      case "object":    schema = typeOrSchema;
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