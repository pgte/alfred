module.exports = function(target) {
  var objs = Array.prototype.slice.call(arguments, 1);
  objs.forEach(function (o) {
      Object.keys(o).forEach(function (k) {
          target[k] = o[k];
      });
  });
  return target;
}
