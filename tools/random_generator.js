var random = module.exports.random = function(max) {
  return Math.floor(Math.random() * max);
};

var createRandomString = module.exports.createRandomString = function(string_length) {
  if (string_length == 0) {
    string_length = 6;
  }
  var chars = "abcdefghijklmnopqrstuvwxyzÇç'àáÁÀèÉÈìíÍÌòóÓÒúùÚÙ\"";
  var randomstring = '';
  for (var i=0; i<string_length; i++) {
    var rnum = Math.floor(Math.random() * chars.length);
    randomstring += chars.substring(rnum,rnum+1);
  }
  return randomstring;
};

module.exports.createRandomObject = function() {
  return {
    a: createRandomString(random(10)),
    b: createRandomString(random(100)),
    c: createRandomString(random(100))
  };
};
