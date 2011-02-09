all: test

mkdirtmp:
	mkdir -p tmp/db
	mkdir -p tmp/db2

mkdirresults:
	mkdir -p benchmarks/results
	mkdir -p benchmarks/results/runs
	mkdir -p benchmarks/results/summaries

clean:
	rm -rf tmp

test: mkdirtmp
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_reload test_key_map_each_with_pos test_indexed_key_map test_indexed_key_map_reload \
	test_cached_key_map test_unordered_index test_unbuffered_collection test_flush_on_exit test_interleaved test_atomic test_atomic_on_null test_compact test_nulls_on_keymap \
	test_nulls_on_unordered_indexes test_nulls_on_ordered_indexes test_false_on_indexes test_ranges_on_ordered_indexes test_meta test_meta_indexes \
	test_file_close test_stream test_exit test_end_flush \
	operators/test_gte operators/test_gt operators/test_lte operators/test_lt operators/test_eq operators/test_composed operators/test_optimization operators/test_in \
	operators/test_nin operators/test_neq operators/test_or operators/test_global_or operators/test_order operators/test_desc_order operators/test_chainable \
	operators/test_find_stream operators/test_find_stream_chained \
	recovery/collection_recovery_test \
	replication/test_master replication/test_slave replication/test_slave_reconnect replication/test_master_temp_roll replication/test_master_seek \
	model/test_create model/test_properties model/test_inspect model/test_validations model/test_destroy model/test_atomic model/test_find model/test_events

benchmark: mkdirtmp mkdirresults
	node tools/benchmarks.js benchmark_find benchmark_collection benchmark_collection_filter benchmark_key_map benchmark_key_map_each_with_pos benchmark_indexed_key_map \
	benchmark_indexed_key_map_random benchmark_cached_key_map_random benchmark_unordered_index benchmark_cached_unordered_index \
	benchmark_cached_ordered_index > benchmarks/results/runs/`date  "+%Y%m%d%H%M%S"`

aggregate_benchmarks: mkdirresults
	node tools/aggregate_benchmarks.js > benchmarks/results/summaries/`date  "+%Y%m%d%H%M%S"`
	cd benchmarks/results/summaries && rm -f latest && ln -s `ls -t1 | head -n1` latest && cd ../../..

publish: clean
	npm publish .

.PHONY: test