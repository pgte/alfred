module.exports.unite = function(a_keys, b_keys) {
  var ret = a_keys || [];
  var key;
  if (b_keys) {
    for(var i = 0; i < b_keys.length; i ++) {
       key = b_keys.k;
       if (!ret.some(function(rec) {
         rec.k == key;
       })) {
         ret.push(b_keys[i]);
       }
    }
  }
  return ret;
};