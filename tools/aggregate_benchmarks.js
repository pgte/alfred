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

var fs = require('fs');

var source_dir = __dirname + '/../benchmarks/results/runs';

var cases = {};

var processMetric = function(date, _case, benchmark, metric_name, value, callback) {
  var day = date.toJSON();
  if (!cases[_case]) {
    cases[_case] = {};
  }
  if (!cases[_case][benchmark]) {
    cases[_case][benchmark] = {};
  }
  if (!cases[_case][benchmark][metric_name]) {
    cases[_case][benchmark][metric_name] = {};
  }
  cases[_case][benchmark][metric_name][day] = value;
  callback(null);
};

var processBenchmark = function(when, benchmark, callback) {
  var _case = benchmark.benchmark;
  var results_done = 0;
  var next_callback = function(err) {
    if (err) {
      callback(err);
    } else {
      results_done ++;
      if (results_done == 4) {
        callback(null);
      }
    }
  };
  benchmark.results.forEach(function(result) {
    var benchmark_name = result.name;
    processMetric(when, _case, benchmark_name, 'rss_before', result.mem_usage_before.rss, next_callback);
    processMetric(when, _case, benchmark_name, 'ellapsed', result.ellapsed, next_callback);
    processMetric(when, _case, benchmark_name, 'rss_after', result.memory_usage_after.rss, next_callback);
    processMetric(when, _case, benchmark_name, 'rss_diff', result.rss_diff, next_callback);
    processMetric(when, _case, benchmark_name, 'throughput', result.throughput, next_callback);
  });
};

var processFile = function(file, callback) {
  var file_full_path = source_dir + '/' + file;
  var date = new Date(parseInt(file.substring(0, 4), 10), parseInt(file.substring(4, 6), 10)-1, parseInt(file.substring(6, 8), 10), parseInt(file.substring(8, 10), 10), parseInt(file.substring(10, 12), 10), parseInt(file.substring(12, 14), 10), 0);
  fs.readFile(file_full_path, 'utf8', function(err, file_data) {
    if (err) {
      callback(err);
    } else {
      var benchmarks = JSON.parse(file_data);
      var benchmarks_done = 0;
      benchmarks.forEach(function(benchmark) {
        processBenchmark(date, benchmark, function(err) {
          if (err) {
            callback(err);
          } else {
            benchmarks_done ++;
            if (benchmarks_done == benchmarks.length) {
              callback(null);
            }
          }
        });
      });
    }
  });
};

fs.readdir(source_dir, function(err, files) {
  if (err) {
    throw err;
  }
  var files_done = 0;
  files.forEach(function(file) {
    processFile(file, function(err) {
      if (err) {
        throw err;
      }
      files_done ++;
      if (files_done == files.length) {
        console.log(JSON.stringify(cases));
      }
    });
  });
});