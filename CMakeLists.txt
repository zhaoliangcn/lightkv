cmake_minimum_required(VERSION 3.16)
project(LightKV VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -DNDEBUG -march=native)
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0 -fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

set(LIGHTKV_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

set(LIGHTKV_SOURCES
    src/slice.cpp
    src/status.cpp
    src/wal.cpp
    src/bloom_filter.cpp
    src/block.cpp
    src/block_builder.cpp
    src/table_builder.cpp
    src/sstable.cpp
    src/cache.cpp
    src/memtable.cpp
    src/compaction.cpp
    src/db_impl.cpp
    src/iterator.cpp
)

add_library(lightkv ${LIGHTKV_SOURCES})
target_include_directories(lightkv
    PUBLIC
        $<BUILD_INTERFACE:${LIGHTKV_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
)
target_link_libraries(lightkv PUBLIC Threads::Threads)

option(LIGHTKV_BUILD_TESTS "Build LightKV tests" ON)

if(LIGHTKV_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

install(TARGETS lightkv
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/ DESTINATION include)