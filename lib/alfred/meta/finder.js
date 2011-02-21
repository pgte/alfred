var fs = require('fs');
var index_key_unitor = require('../util/index_key_unitor');
var FinderStream = require('./finder_stream');

var operators = 
  (function() {
    var operators = fs.readdirSync(__dirname + '/operators');
    var operatorModules = {};
    operators.forEach(function(operator) {
      var operatorModuleName = operator.replace(/\.js$/, '');
      operatorModules[operatorModuleName] = require('./operators/' + operator);
    });
    return operatorModules;
  })();

var Finder = function(key_map, query) {
  this.key_map = key_map;
  this.query = query;
  /*
  this.order = order;
  if (callback) {
    this.execute(this.query, callback);
  }
  */
};

Finder.prototype.executeOperatorOnField = function(keys, field, operator_string, value) {
  var index_name = field;
  if (index_name.length < 1) { throw new Error('invalid index name: ' + index_name); }
  var index = this.key_map[index_name];
  if (!index) { throw new Error('could not find index ' + index_name); }
  var operator = operators[operator_string];
  if (!operator) { throw new Error('could not find operator ' + operator_string); }
  return operator.operateOnIndex(keys, index, value, field, this);
};

var _optimize = function(query) {
  var self = this,
      field;
      
  for (field in this.query) {
    if (query.hasOwnProperty(field)) {
      (function(field) {
        var condition = query[field];
        var operators = [];
        for (var operator in condition) {
          if (condition.hasOwnProperty(operator)) {
            (function(operator) {
              operators.push(operator.trim());
            })(operator);
          }
        }
        
        if (operators.length == 2) {
          operators.sort();
          var first_op = operators[0];
          var second_op = operators[1];
          if (   (first_op == '$gt' || first_op == '$gte') &&
                 (second_op == '$lt' || second_op == '$lte')) {
            query[field] = {"$range" : [condition[first_op], condition[second_op], (first_op == '$gt'), (second_op == '$lt')]};
          }
        }
        
      })(field);
    }
  }
  return query;
};

Finder.prototype.executeCondition = function(keys, field, condition) {
  var self = this;
  for (var operator in condition) {
    if (condition.hasOwnProperty(operator)) {
      (function(operator) {
        var value = condition[operator];
        keys = self.executeOperatorOnField(keys, field, operator, value);
      })(operator);
    }
  }
  return keys;
};

Finder.prototype.executeAndJustReturnKeys = function(query) {
  var self = this;
  var keys;
  _optimize(query);
  for (var field in query) {
    if (query.hasOwnProperty(field)) {
      (function(field) {
        var condition = query[field];
        keys = self.executeCondition(keys, field, condition);
      })(field);
    }
  }
  return keys;
};

Finder.prototype.executeAllAndJustReturnKeysInOrder = function(query, order, offset, limit, callback) {
  var self = this;
  var keys;
  if (!query) {
    query = this.query;
  }
  
  if (!query) {
    callback(new Error('no query to execute'));
    return;
  }
  
  try {
    if (Array.isArray(query)) { // We got an array, meaning an OR condition
      keys = [];
      self.query.forEach(function(subquery) {
        var new_keys = self.executeAndJustReturnKeys(subquery);
        keys = index_key_unitor.unite(keys, new_keys);
      });
      
    } else {
      keys = self.executeAndJustReturnKeys(query);
    }
    
    // order keys
    if (order) {
      var orders = order.split(" ");
      var index_name = orders[0];
      var asc = orders[1] || 'asc';
      asc = asc.toLowerCase();
      if (asc != 'asc' && asc != 'desc') {
        throw new Error('Unknown order clause \'' + asc + '\'');
      }
      var index = this.key_map.indexes[index_name];
      if (!index) {
        throw new Error('invalid order clause. Index with name \'' + index_name + '\' not found.');
      }
      if (!index.ordered) {
        throw new Error('Index \'' + index_name + '\' is not ordered');
      }
      var ordered_keys = [];
      var each_handler = function(key, value) {
        if (keys.some(function(in_key) {
          return in_key.k == value.k;
        })) {
          ordered_keys.push(value);
        }
      };
      if (asc == 'asc') {
        index.each(each_handler);
      } else {
        index.eachReverse(each_handler);
      }
      keys = ordered_keys;
    }
    
    // offset
    if (offset) {
      keys.splice(0, offset);
    }
    
    // limit
    if (limit) {
      keys.splice(limit);
    }
    
    callback(null, keys);
    
  } catch(excep) {
    callback(excep);
  }
};

Finder.prototype.execute = function(callback) {
  var self = this;
  // transform the keys into real objects
  this.executeAllAndJustReturnKeysInOrder(this.query, this.order, this.offset, this.limit, function(err, keys) {
    if (err) {
      callback(err);
      return;
    }
    keys.forEach(function(key, index) {
      self.key_map.getAtPos(key.p, key.l, function(err, key, value) {
        if (err) { callback(err); return; }
        callback(null, key, value);
      });
    });
  });
};

Finder.prototype.executeBulk = function(callback) {
  var self = this;
  this.executeAllAndJustReturnKeysInOrder(this.query, this.order, this.offset, this.limit, function(err, keys) {
    if (err) {
      callback(err);
      return;
    }
    var results = [];   
    var got = 0; 
    if (keys && keys.length > 0) {
      keys.forEach(function(key, index) {
        self.key_map.getAtPos(key.p, key.l, function(err, key, value) {
          if (err) { callback(err); return; }
          results.push({key: key, value: value});
          got ++;
          if (got == keys.length) {
            callback(null, results);
          }
        });
      });
    } else {
      callback(null, []);
    }
  });
};

module.exports.create = function(key_map, query) {
  var finder = new Finder(key_map, query);
  var chainable = function(callback) {
    return chainable.execute(callback);
  };
  
  chainable.execute = function(callback) {
    finder.execute(callback);
    return chainable;
  };
  
  chainable.order = function(order) {
    finder.order = order;
    return chainable;
  };
  
  chainable.reset = function() {
    finder.query = null;
    return chainable;
  };
  
  chainable.where = function(query) {
    finder.query = query;
    return chainable;
  };
  
  chainable.or = function(query) {
    if (!Array.isArray(finder.query)) {
      finder.query = [finder.query];
    }
    finder.query.push(query);
    return chainable;
  };
  
  chainable.limit = function(limit) {
    finder.limit = limit;
  };

  chainable.offset = function(offset) {
    finder.offset = offset;
  };
  
  chainable.first = function() {
    finder.limit = 1;
  };
  
  chainable.bulk = function(callback) {
    return chainable.all(callback);
  };
  
  chainable.all = function(callback) {
    finder.executeBulk(callback);
    return chainable;
  };
  
  chainable.stream = function(callback) {
    var stream = FinderStream.open(JSON.parse(JSON.stringify(finder.query)), finder.order, finder.offset, finder.limit, finder);
    if (callback) {
      callback(stream);
      return chainable;
    } else {
      return stream;
    }
  };
  
  chainable.setKeyMap = function(key_map) {
    finder.key_map = key_map;
  };
  
  return chainable;
};

