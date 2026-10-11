# Test targets.
#
# IMPORTANT: tests verify correctness with bare assert(), so NDEBUG must stay
# OFF for every test target. A Release build of the library defines NDEBUG, and
# inheriting it here would silently turn every assertion into a no-op — the tests
# would report success while checking nothing.
#
# The library itself contains no assert() (only raft.cpp keys log verbosity off
# NDEBUG), so leaving NDEBUG enabled for `lightkv` is safe.

if(MSVC)
    set(LIGHTKV_TEST_UNDEBUG /UNDEBUG)
else()
    set(LIGHTKV_TEST_UNDEBUG -UNDEBUG)
endif()

# lightkv_add_test(<target> <source>)
#   Builds <source> into <target>, links the library and registers a CTest entry.
function(lightkv_add_test target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE lightkv)
    target_compile_options(${target} PRIVATE ${LIGHTKV_TEST_UNDEBUG})
    add_test(NAME ${target} COMMAND ${target})
endfunction()

# lightkv_add_tool(<target> <source>)
#   Same as above but not registered with CTest — manual/interactive binaries.
function(lightkv_add_tool target source)
    add_executable(${target} ${source})
    target_link_libraries(${target} PRIVATE lightkv)
    target_compile_options(${target} PRIVATE ${LIGHTKV_TEST_UNDEBUG})
endfunction()

# ── Unit tests ────────────────────────────────────────────────────────────
lightkv_add_test(skiplist_test skiplist_test.cpp)
lightkv_add_test(memtable_test memtable_test.cpp)
lightkv_add_test(wal_test wal_test.cpp)
lightkv_add_test(bloom_test bloom_test.cpp)
lightkv_add_test(sstable_test sstable_test.cpp)
lightkv_add_test(block_test block_test.cpp)
lightkv_add_test(db_test db_test.cpp)
lightkv_add_test(server_test test_server.cpp)
lightkv_add_test(p0_test test_p0_cmds.cpp)
lightkv_add_test(p1_test test_p1_cmds.cpp)
lightkv_add_test(p2_test test_p2_cmds.cpp)
lightkv_add_test(zset_test test_zset.cpp)
lightkv_add_test(geo_test test_geo.cpp)
lightkv_add_test(auth_test test_auth.cpp)
lightkv_add_test(raft_test raft_test.cpp)
lightkv_add_test(cluster_test cluster_test.cpp)

# ── Benchmarks / stress / manual launchers (not part of `ctest`) ───────────
lightkv_add_tool(benchmark_test benchmark_test.cpp)
lightkv_add_tool(bench_cpp bench_cpp.cpp)
lightkv_add_tool(bench_pipeline_cpp bench_pipeline_cpp.cpp)
lightkv_add_tool(stress_cpp stress_cpp.cpp)
lightkv_add_tool(run_server run_server.cpp)
lightkv_add_tool(run_server_st run_server_st.cpp)
