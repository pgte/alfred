all: test

test:
	node tools/test.js test_collection test_collection_filter test_key_map test_key_map_with_pos

.PHONY: test