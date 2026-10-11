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

// The temp root is created once per process, before main(). Several tests open
// files directly under it (without going through DB::Open, which would create
// the directory itself) and would otherwise fail with ENOENT on a clean machine.
namespace lightkv_test_paths {
struct EnsureTmpDir {
    EnsureTmpDir() {
        std::error_code ec;
        std::filesystem::create_directories(LIGHTKV_TEST_TMP, ec);
    }
};
inline const EnsureTmpDir kEnsureTmpDir{};
}  // namespace lightkv_test_paths
