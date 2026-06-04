add_executable(skiplist_test skiplist_test.cpp)
target_link_libraries(skiplist_test PRIVATE lightkv)
add_test(NAME SkipListTest COMMAND skiplist_test)

add_executable(memtable_test memtable_test.cpp)
target_link_libraries(memtable_test PRIVATE lightkv)
add_test(NAME MemTableTest COMMAND memtable_test)

add_executable(wal_test wal_test.cpp)
target_link_libraries(wal_test PRIVATE lightkv)
add_test(NAME WALTest COMMAND wal_test)

add_executable(bloom_test bloom_test.cpp)
target_link_libraries(bloom_test PRIVATE lightkv)
add_test(NAME BloomTest COMMAND bloom_test)

add_executable(sstable_test sstable_test.cpp)
target_link_libraries(sstable_test PRIVATE lightkv)
add_test(NAME SSTableTest COMMAND sstable_test)

add_executable(block_test block_test.cpp)
target_link_libraries(block_test PRIVATE lightkv)
add_test(NAME BlockTest COMMAND block_test)

add_executable(db_test db_test.cpp)
target_link_libraries(db_test PRIVATE lightkv)
add_test(NAME DBTest COMMAND db_test)

add_executable(benchmark_test benchmark_test.cpp)
target_link_libraries(benchmark_test PRIVATE lightkv)

add_executable(server_test test_server.cpp)
target_link_libraries(server_test PRIVATE lightkv)
add_test(NAME ServerTest COMMAND server_test)

add_executable(bench_cpp bench_cpp.cpp)
target_link_libraries(bench_cpp PRIVATE lightkv)