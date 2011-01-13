$(document).ready(function() {
  var min_day, max_day;
  $.getJSON('./summaries/latest', function(benchmarks) {
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
                    var simple_day = day; //.toString().substring(0, 10);
                    if (min_day === undefined || min_day > simple_day) {
                      min_day = simple_day;
                    }
                    if (max_day === undefined || max_day < simple_day) {
                      max_day = simple_day;
                    }
                    if (!metric_results[simple_day]) {
                      metric_results[simple_day] = [];
                    }
                    metric_results[simple_day].push({name: measure_name, measure: measure[day]});
                  }
                }
              }
            }
            
            var array_metric_results = [];
            for (var date in metric_results) {
              array_metric_results.push({date: date, results: metric_results[date]});
            }
            metric_results = array_metric_results.sort(function(a, b) {
              return (new Date(a.date)).getTime() - (new Date(b.date)).getTime();
            });
            
            benchmark_results.push({name: metric_name, results_by_date: metric_results});
          }
        }
        results.push({name: benchmark_name, results: benchmark_results, position: results.length + 1});
      }
    }
    $('#benchmark_template').tmpl(results).appendTo('#benchmarks');
    
    // min_day = new Date(new Date(min_day).getTime() - (2 * 3600 * 24 * 1000));
    // max_day = new Date(new Date(max_day).getTime() + (2 * 3600 * 24 * 1000));
    
    setTimeout(function() {
      for (var benchmark_name in benchmarks) {
        var benchmark = benchmarks[benchmark_name];
        for (var result_name in benchmark) {
          var result = benchmark[result_name];
          for (var metric_name in result) {
            var metric = result[metric_name];
            var metric_plot_data = [];
            for (var day in metric) {
              var one_result = metric[day];
              metric_plot_data.push([Date.parse(day), one_result])
            }
            metric_plot_data = metric_plot_data.sort(function(a, b) {
              return a[0] - b[0];
            });
            
            var options = {
              lines: { show: true },
              points: { show: true},
              xaxis: {
                mode: 'time',
                minTickSize: [1, "day"],
                min: (new Date(min_day)).getTime(),
                max: (new Date(max_day)).getTime()              }
            };
            var plot_in = '#'+benchmark_name + '_' + result_name.replace(/( |\.|\$)/g, '_') + '_' + metric_name.replace(/ /g, '_');
            $.plot($(plot_in), [metric_plot_data], options);
            
          }

        }

      }
      
    }, 0);
    
    
  });
});