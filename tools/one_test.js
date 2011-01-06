#!/usr/bin/env node

var path = require('path');

var test_path = path.join(__dirname, '..', 'test', process.argv[2] + '.js');
if (!path.existsSync(test_path)) throw "Could not find test path " + test_path;

var test_module = require(test_path);

var exiting = false;
var do_exit = function(no_exit) {
  if (exiting) return;
  exiting = true;
  if (test_module.teardown) test_module.teardown();
  process.removeListener('exit', abnormal_process_exit);
  if (!no_exit) {
    process.nextTick(function() {
      process.exit();
    });
  }
}

var exception_handler = function(excp) {
  console.log('EXCEPTION CAUGHT DURING TEST:');
  if (excp.message || excp.name) {
    if (excp.name) process.stdout.write(excp.name + " ");
    if (excp.message) process.stdout.write(excp.message + " ");
  } else {
    var util = require('util');
    process.stdout.write(util.inspect(excp));
  }
  if (excp.backtrace) process.stdout.write(excp.backtrace + " ");
  if (excp.stack) process.stdout.write("stack: " + excp.stack + " ");
  var util = require('util');
  process.stdout.write(". Inspecting error: \n" + util.inspect(excp) + " ");    
  process.stdout.write("\n");
  do_exit();
};

if (!test_module.run) throw "test module " + module_path + " does not export run() function";
process.on('uncaughtException', exception_handler);

var abnormal_process_exit = function() {
  console.log('process exited abnormally')
  do_exit();
};
process.on('exit', abnormal_process_exit);

if (test_module.setup) {
  test_module.setup(function(err, no_exit) {
    if (err) {
      exception_handler(err);
    }
    test_module.run(function(err) {
      if (err) {
        exception_handler(err);
      }
      do_exit(no_exit);
    });
  });
} else {
  test_module.run(function(err, no_exit) {
    if (err) {
      exception_handler(err);
    }
    do_exit(no_exit);
  });
}