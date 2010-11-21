var path = require('path');

var test_path = path.join(__dirname, '..', 'benchmarks', process.argv[2] + '.js');
if (!path.existsSync(test_path)) throw "Could not find test path " + test_path;

var test_module = require(test_path);

var benchmarks = [];
var current_benchmark;

var getCurrentMilliseconds = function() {
  d = new Date();
  return d.getTime();
}
var benchmark = {
  start: function(name, count) {
    if (!count) {
      count = 1;
    }
    var memory_usage = process.memoryUsage();
    current_benchmark = {
      name: name,
      started: getCurrentMilliseconds(),
      count: count,
      mem_usage_before: memory_usage
    };
  },
  end: function() {
    current_benchmark.ended = getCurrentMilliseconds();
    current_benchmark.ellapsed = current_benchmark.ended - current_benchmark.started;
    current_benchmark.average = current_benchmark.ellapsed / current_benchmark.count;
    current_benchmark.throughput = Math.floor(1000 / current_benchmark.average);
    current_benchmark.memory_usage_after = process.memoryUsage();
    benchmarks.push(current_benchmark);
    current_benchmark = null;
  }
};

test_module.run(benchmark, function() {
  benchmarks.forEach(function(benchmark) {
    console.log(benchmark);
  });
});