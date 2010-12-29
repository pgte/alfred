$(document).ready(function() {
  $.getJSON('/summaries/latest', function(benchmarks) {
    var results = [];
    var result, interm_result;
    for(var benchmark_name in benchmarks) {
      var benchmark_results = [];
      if (benchmarks.hasOwnProperty(benchmark_name)) {
        var benchmark = benchmarks[benchmark_name];
        for(var metric_name in benchmark) {
          if (benchmark.hasOwnProperty(metric_name)) {
            var metric = benchmark[metric_name];
            var metric_results = {};
            
            // parse results for each metric and group them by date
            for(var measure_name in metric) {
              if (metric.hasOwnProperty(measure_name)) {
                var measure = metric[measure_name];
                for (var day in measure) {
                  if (measure.hasOwnProperty(day)) {
                    var simple_day = day.toString().substring(0, 10);
                    if (!metric_results[simple_day]) {
                      metric_results[simple_day] = [];
                    }
                    metric_results[simple_day].push({name: measure_name, measure: measure[day]});
                  }
                }
              }
            }
            
            benchmark_results.push({name: metric_name, results_by_date: metric_results});
          }
        }
        results.push({name: benchmark_name, results: benchmark_results});
      }
    }
    $('#benchmark_template').tmpl(results).appendTo('#benchmarks');
  });
});