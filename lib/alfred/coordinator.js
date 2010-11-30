var Coordinator = function() {
};

module.exports.klass = Coordinator;

Coordinator.prototype.notify = function(event, args) {
  var func_name = 'notify_' + event;
  var func = this[func_name];
  if (func) {
    func.apply(this, args);
  }
};