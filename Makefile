all: test

test:
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_each_with_pos test_indexed_key_map test_indexed_key_map_reload \
	test_cached_key_map test_functional_index test_unbuffered_collection

benchmark:
	node tools/benchmarks.js benchmark_collection benchmark_collection_filter benchmark_key_map benchmark_key_map_each_with_pos benchmark_indexed_key_map \
	benchmark_indexed_key_map_random benchmark_cached_key_map_random benchmark_functional_index benchmark_cached_functional_index > benchmarks/results/`date  "+%Y%m%d%H%M%S"`

.PHONY: test