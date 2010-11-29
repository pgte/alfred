all: test

mkdirtmp:
	mkdir -p tmp

mkdirresults:
	mkdir -p benchmarks/results
	mkdir -p benchmarks/results/runs
	mkdir -p benchmarks/results/summaries

test: mkdirtmp
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_each_with_pos test_indexed_key_map test_indexed_key_map_reload \
	test_cached_key_map test_functional_index test_unbuffered_collection test_flush_on_exit test_interleaved

benchmark: mkdirtmp mkdirresults
	node tools/benchmarks.js benchmark_collection benchmark_collection_filter benchmark_key_map benchmark_key_map_each_with_pos benchmark_indexed_key_map \
	benchmark_indexed_key_map_random benchmark_cached_key_map_random benchmark_functional_index benchmark_cached_functional_index > benchmarks/results/runs/`date  "+%Y%m%d%H%M%S"`

aggregate_benchmarks: mkdirresults
	node tools/aggregate_benchmarks.js > benchmarks/results/summaries/`date  "+%Y%m%d%H%M%S"`

.PHONY: test