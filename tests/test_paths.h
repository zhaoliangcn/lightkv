// Cross-platform temp paths and cleanup helpers for tests.
// Include this header instead of hardcoding "C:/lightkv_tmp" or "/tmp".
#pragma once

#include <filesystem>
#include <string>

#ifdef _WIN32
#define LIGHTKV_TEST_TMP "C:/lightkv_tmp"
#else
#define LIGHTKV_TEST_TMP "/tmp/lightkv_test"
#endif

// Portable recursive delete (replaces system("rm -rf ..."))
inline void lightkv_remove_tree(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}
