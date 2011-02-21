var defaultErrorMessages = {
  "minLength": "is too short",
  "maxLength": "is too long",
  "optional": "is not optional",
  "type": "is of the wrong type",
  "enum": "is invalid",
  "patten": "is invalid",
  "validateWith": "is invalid"
};

var Validator = function(object, schema) {
  this.errors = [];

  if (typeof(object) !== 'object' || typeof(schema) !== 'object') {
      throw new(TypeError)("`validate` takes two objects as arguments");
  }
  
  this.object = object;
  this.schema = schema;

  this._validateObject();

};

module.exports.validate = function (object, schema) {
  var validator = new Validator(object, schema);
  return { valid: !Boolean(validator.errors.length), errors: validator.errors };
};

Validator.prototype._validateObject = function () {
  var self = this;
  Object.keys(self.schema.properties).forEach(function (k) {
    self._validateProperty(k, self.schema.properties[k]);
  });
};

var checkType = function (val, type) {
  switch (type) {
    case 'string':
      return typeof(val) === 'string';
    case 'array':
      return Array.isArray(val);
    case 'object':
      return val && (typeof(val) === 'object') && !Array.isArray(val);
    case 'number':
      return typeof(val) === 'number';
    case 'integer':
      return typeof(val) === 'number' && (val % 1 === 0);
    case 'null':
      return val === null;
    case 'boolean':
      return typeof(val) === 'boolean';
    case 'any':
      return typeof(val) !== 'undefined';
    default:
      return true;
  }
};

Validator.prototype._validateProperty = function (property, schema) {
  var self = this;
  var object = this.object;
  var type, value = object[property];

  function constrain(name, value, assert) {
    if ((name in schema) && !assert(value, schema[name])) {
      self.error(name, property, value, schema);
    }
  }

  if (value === undefined && !schema.optional) {
    this.error('optional', property, true, schema);
  }
  if (schema.enum && schema.enum.indexOf(value) === -1) {
    this.error('enum', property, value, schema);
  }
  if (schema.requires && object[schema.requires] === undefined) {
    this.error('requires', property, null, schema);
  }
  if (checkType(value, schema.type)) {
    constrain('validateWith', value, function(a, e) { return e(a); });
    switch (schema.type || typeof(value)) {
      case 'string':
        constrain('minLength', value.length, function (a, e) { return a >= e; });
        constrain('maxLength', value.length, function (a, e) { return a <= e; });
        constrain('pattern',   value,        function (a, e) { return e.test(a); });
        break;
      case 'number':
        constrain('minimum',     value, function (a, e) { return a >= e; });
        constrain('maximum',     value, function (a, e) { return a <= e; });
        constrain('divisibleBy', value, function (a, e) { return a % e === 0; });
    }
  } else {
    this.error('type', property, typeof(value), schema);
  }

};

Validator.prototype.error = function(attribute, property, actual, schema) {
  var message = schema.messages && schema.messages[attribute] || defaultErrorMessages[attribute] || "no default message";
  var expected = schema[attribute];
  
  this.errors.push({
    attribute: attribute,
    property: property,
    expected: (!(expected instanceof RegExp) && (typeof expected == 'function')) ? 'Function' : expected,
    actual: actual,
    message: message
  });
};