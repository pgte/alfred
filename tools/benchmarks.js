process.on('uncaughtException', function(excp) {
  if (excp.message || excp.name) {
    if (excp.name) process.stdout.write(excp.name);
    if (excp.message) process.stdout.write(excp.message);
    if (excp.backtrace) process.stdout.write(excp.backtrace);
    if (excp.stack) process.stdout.write(excp.stack);
  } else {
    sys = require('sys');
    process.stdout.write(sys.inspect(excp));    
  }
});

var benchmarks = process.argv.slice(2);
if(benchmarks.length < 1) {
  console.log('No benchmarks to run');
  process.exit(1);
}

var path          = require('path'),
    child_process = require('child_process'),
    assert        = require('assert');

var benchmark_index = 0;

var benchmark_results = [];

var do_benchmark = function() {
  var benchmark = benchmarks[benchmark_index];
  
  var module_path = path.join(__dirname, '..', 'benchmarks', benchmark + ".js");
  if (!path.existsSync(module_path)) throw new Error("Could not find benchmark path "+module_path);
  var run_this = [path.join(__dirname, 'benchmark.js'), benchmark];
  var benchmark_child = child_process.spawn(process.argv[0], run_this, {env: process.env});
  var child_output = '';
  benchmark_child.stdout.on('data', function(data) {
    child_output += data.toString();
  });
  benchmark_child.stderr.on('data', function(data) {
    assert.ok(false, data.toString());
  });
  benchmark_child.on('exit', function() {
    benchmark_index ++;
    var res;
    try {
      res = JSON.parse(child_output)
    } catch(excp) {
      res = child_output;
    }
    benchmark_results.push({
      benchmark: benchmark,
      results: res
    });
    
    if (benchmark_index < benchmarks.length) {
      setTimeout(do_benchmark, 1000);
    } else {
      console.log(JSON.stringify(benchmark_results));
    }
  });

};
do_benchmark();