module.exports.intercept = function(a_keys, b_keys) {
  if (a_keys) {
    if ( ! b_keys ) return null;
    return b_keys.filter(function(b_key) {
      return a_keys.some(function(a_key) {
        return a_key.k == b_key.k;
      });
    });
    
  } else {
    return b_keys;
  }
};