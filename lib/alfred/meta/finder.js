var fs = require('fs');
var index_key_unitor = require('../util/index_key_unitor');

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

var Finder = function(key_map, query, order, callback) {
  this.key_map = key_map;
  this.query = query;
  this.order = order;
  if (callback) {
    this.execute(this.query, callback);
  }
};

module.exports.create = function(key_map, query, order, callback) {
  return new Finder(key_map, query, order, callback);
};

Finder.prototype.executeOperatorOnField = function(keys, field, operator_string, value) {
  var index_name = field;
  if (index_name.length < 1) { throw new Error('invalid index name: ' + index_name); }
  var index = this.key_map[index_name];
  if (!index) { throw new Error('could not find index ' + index_name); }
  var operator = operators[operator_string];
  if (!operator) { throw new Error('could not find operator ' + operator_string); }
  return operator.operateOnIndex(keys, index, value, field, this);
}

var _optimize = function(query) {
  var self = this;
  for (var field in this.query) {
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
          if (   (first_op == '$gt' || first_op == '$gte')
              && (second_op == '$lt' || second_op == '$lte')) {
            query[field] = {"$range" : [condition[first_op], condition[second_op], (first_op == '$gt'), (second_op == '$lt')]}
          }
        }
        
      })(field);
    }
  }
  return query;
};

Finder.prototype.executeCondition = function(keys, field, condition) {
  // console.log("field: " + field);
  // console.log(condition);
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
    };
  }
  return keys;
}

Finder.prototype.execute = function(query, callback) {
  var self = this;
  var keys;
  if (!query) {
    query = this.query;
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
    if (self.order) {
      var orders = self.order.split(" ");
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
    
    // transform the keys into real objects
    keys.forEach(function(key, index) {
      self.key_map.getAtPos(key.p, key.l, function(err, key, value) {
        if (err) { callback(err); return; }
        callback(null, key, value);
      });
    });
  } catch(excep) {
    callback(excep);
  }
};
