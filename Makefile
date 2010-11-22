all: test

test:
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_with_pos test_indexed_key_map

benchmark:
	node tools/benchmark_all.js benchmark_collection benchmark_collection_filter benchmark_key_map benchmark_key_map_each_with_pos

.PHONY: test