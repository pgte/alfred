var fs = require('fs');
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

var Finder = function(key_map, query, callback) {
  this.key_map = key_map;
  this.query = query;
  if (callback) {
    this.execute(callback);
  }
};

module.exports.create = function(key_map, query, callback) {
  return new Finder(key_map, query, callback);
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

Finder.prototype._optimize = function() {
  var self = this;
  for (var field in this.query) {
    if (self.query.hasOwnProperty(field)) {
      (function(field) {
        var condition = self.query[field];
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
            self.query[field] = {"$range" : [condition[first_op], condition[second_op], (first_op == '$gt'), (second_op == '$lt')]}
          }
        }
        
      })(field);
    }
  }
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

Finder.prototype.execute = function(callback) {
  var self = this;
  try {
    this._optimize();
    var self = this;
    var keys;
    for (var field in this.query) {
      if (self.query.hasOwnProperty(field)) {
        (function(field) {
          var condition = self.query[field];
          keys = self.executeCondition(keys, field, condition);
        })(field);
      };
    }
    keys.forEach(function(key, index) {
      self.key_map.get_at_pos(key.p, key.l, function(err, key, value) {
        if (err) { callback(err); return; }
        callback(null, key, value);
      });
    });
  } catch(excep) {
    callback(excep);
  }
};
