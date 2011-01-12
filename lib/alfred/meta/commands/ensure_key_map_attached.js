var EnsureKeyMapAttachedCommand = function(key_map_name, options) {
  this.key_map_name = key_map_name;
  this.options = options;
};

var create = function(key_map_name, options) {
  return new EnsureKeyMapAttachedCommand(key_map_name, options);
}

module.exports.register = function(meta) {
  meta.registerCommand(['ensure_key_map_attached', 'ensureKeyMapAttached', 'ensure'], create);
};

EnsureKeyMapAttachedCommand.prototype.do = function(meta, callback) {
  if (!meta[this.key_map_name]) {
    meta.attach_key_map(this.key_map_name, this.options, callback);
  } else {
    callback(null, meta[this.key_map_name]);
  }
};