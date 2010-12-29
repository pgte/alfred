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

var path = require('path');

var benchmark_path = path.join(__dirname, '..', 'benchmarks', process.argv[2] + '.js');
if (!path.existsSync(benchmark_path)) throw "Could not find benchmark path " + benchmark_path;

var benchmark_module = require(benchmark_path);

var benchmarks = [];
var current_benchmark;

var benchmark = {
  start: function(name, count) {
    if (!count) {
      count = 1;
    }
    var memory_usage = process.memoryUsage();
    current_benchmark = {
      name: name,
      started: Date.now(),
      count: count,
      mem_usage_before: memory_usage
    };
  },
  end: function() {
    current_benchmark.ended = Date.now();
    current_benchmark.ellapsed = current_benchmark.ended - current_benchmark.started;
    current_benchmark.average = current_benchmark.ellapsed / current_benchmark.count;
    current_benchmark.throughput = Math.floor(1000 / current_benchmark.average);
    current_benchmark.memory_usage_after = process.memoryUsage();
    current_benchmark.rss_diff = current_benchmark.memory_usage_after.rss - current_benchmark.mem_usage_before.rss;
    benchmarks.push(current_benchmark);
    current_benchmark = null;
  }
};

benchmark_module.run(benchmark, function() {
  if (benchmark.length == 0) {
    console.log("No results collected");
  }
  console.log(JSON.stringify(benchmarks));
});