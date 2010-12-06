var options_merger = require('../util/options_merger'),
    bplustree         = require('../../../dist/bplustree');

var default_options = {
  bplustree_order: 100
};

var OrderedIndex = function(transform_function, options) {  
  this.transform_function = transform_function;
  this.options = options_merger.merge(default_options, options);
  this.bplustree = bplustree.create({
    order: this.options.bplustree_order
  });
  this.map = {};
  this.transformed_map = {};
  this.counter = 0;
};

module.exports.open = function(transform_function, options) {
  return new OrderedIndex(transform_function, options);
};

OrderedIndex.prototype.transformFunction = function() {
  return this.transform_function;
};

OrderedIndex.prototype.put = function(key, value, pos, length) {
  
  if (value) {
    
    var transformed = this.transform_function(value);
    if (transformed) {
      var newRecord = {
        r: transformed,
        k: key,
        p: pos,
        l: length
      };
      var transformed_pointer;

      if (transformed_pointer = this.transformed_map[transformed]) {
        transformed_pointer.push(newRecord);
      } else {
        this.transformed_map[transformed] = transformed_pointer = [newRecord];
        if (!this.map.hasOwnProperty(key)) {
          this.counter ++;
        };
        this.map[key] = transformed_pointer;
        
        this.bplustree.set(transformed, transformed_pointer);
      }
    }
  } else {

    // remove
    var value_pointer;
    // erase previous value
    if (value_pointer = this.map[key]) {
      delete this.map[key];
      this.counter --;
      
      if (transformed_pointer = this.transformed_map[value_pointer.r]) {
        transformed_pointer.forEach(function(transformed, index) {
          if (transformed.k == key) {
            transform.splice(index, 1);
          }
          if (transform.length < 1) {
            delete this.transformed_map[value_pointer.r];
          }
        });
      }
    }
  }
};

OrderedIndex.prototype.filter = function(filter_function, callback){
  var self = this;
  this.bplustree.each(function(key, values) {
    values.forEach(function(value) {
      if (filter_function(value.r)) {
        callback(value.k, value.p, value.l);
      };
    })
  });
};

OrderedIndex.prototype.count = function(filter_function, callback) {
  var count = 0;
  for (transformed in this.transformed_map) {
    if (this.transformed_map.hasOwnProperty(transformed)) {
      if (filter_function(transformed)) {
        count += this.transformed_map[transformed].length;
      }
    }
  }
  callback(null, count);
};
