all: test

mkdirtmp:
	mkdir -p tmp/db

mkdirresults:
	mkdir -p benchmarks/results
	mkdir -p benchmarks/results/runs
	mkdir -p benchmarks/results/summaries

clean:
	rm -rf tmp

test: mkdirtmp
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_each_with_pos test_indexed_key_map test_indexed_key_map_reload \
	test_cached_key_map test_unordered_index test_unbuffered_collection test_flush_on_exit test_interleaved test_atomic test_compact test_nulls_on_indexes \
	test_nulls_on_unordered_indexes test_nulls_on_ordered_indexes test_false_on_indexes test_ranges_on_ordered_indexes test_meta test_meta_indexes test_file_close \
	operators/test_gte operators/test_gt operators/test_lte operators/test_lt operators/test_eq operators/test_composed operators/test_optimization operators/test_in \
	operators/test_nin operators/test_neq operators/test_or

benchmark: mkdirtmp mkdirresults
	node tools/benchmarks.js benchmark_collection benchmark_collection_filter benchmark_key_map benchmark_key_map_each_with_pos benchmark_indexed_key_map \
	benchmark_indexed_key_map_random benchmark_cached_key_map_random benchmark_unordered_index benchmark_cached_unordered_index benchmark_cached_ordered_index > benchmarks/results/runs/`date  "+%Y%m%d%H%M%S"`

aggregate_benchmarks: mkdirresults
	node tools/aggregate_benchmarks.js > benchmarks/results/summaries/`date  "+%Y%m%d%H%M%S"`

.PHONY: test