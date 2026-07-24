#include "lightkv/migrate.h"
#include <iostream>
#include <string>
#include <csignal>

static lightkv::RedisMigrator* g_migrator = nullptr;

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        std::cerr << "\n[Migrator] Received signal " << sig << ", stopping...\n";
        if (g_migrator) {
            auto stats = g_migrator->GetStats();
            std::cerr << "[Migrator] Interrupted at " << stats.migrated_keys
                      << "/" << stats.total_keys << " keys\n";
        }
        exit(0);
    }
}

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --redis-host HOST    Redis server host (default: 127.0.0.1)\n"
              << "  --redis-port PORT    Redis server port (default: 6379)\n"
              << "  --redis-auth PWD     Redis password (default: none)\n"
              << "  --target-host HOST   LightKV target host (default: 127.0.0.1)\n"
              << "  --target-port PORT   LightKV target port (default: 6380)\n"
              << "  --prefix PREFIX      Migrate keys with prefix only (default: all)\n"
              << "  --batch N            SCAN batch size (default: 1000)\n"
              << "  --help               Show this help\n";
}

int main(int argc, char* argv[]) {
    std::string redis_host = "127.0.0.1";
    uint16_t redis_port = 6379;
    std::string redis_auth;
    std::string target_host = "127.0.0.1";
    uint16_t target_port = 6380;
    std::string prefix;
    size_t batch = 1000;

    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--redis-host" && i + 1 < argc) {
            redis_host = argv[++i];
        } else if (arg == "--redis-port" && i + 1 < argc) {
            redis_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--redis-auth" && i + 1 < argc) {
            redis_auth = argv[++i];
        } else if (arg == "--target-host" && i + 1 < argc) {
            target_host = argv[++i];
        } else if (arg == "--target-port" && i + 1 < argc) {
            target_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--prefix" && i + 1 < argc) {
            prefix = argv[++i];
        } else if (arg == "--batch" && i + 1 < argc) {
            batch = static_cast<size_t>(std::stoul(argv[++i]));
        } else {
            std::cerr << "[Migrator] Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cerr << "[Migrator] Redis → LightKV Migration Tool\n";
    std::cerr << "[Migrator] Redis: " << redis_host << ":" << redis_port << "\n";
    std::cerr << "[Migrator] Target: " << target_host << ":" << target_port << "\n";

    lightkv::RedisMigrator migrator(
        redis_host, redis_port,
        target_host, target_port,
        redis_auth, batch);

    g_migrator = &migrator;

    if (!migrator.Connect()) {
        std::cerr << "[Migrator] Failed to connect\n";
        return 1;
    }

    int64_t migrated;
    if (prefix.empty()) {
        std::cerr << "[Migrator] Starting full migration...\n";
        migrated = migrator.MigrateAll();
    } else {
        std::cerr << "[Migrator] Starting prefix migration: '" << prefix << "*'\n";
        migrated = migrator.MigrateByPrefix(prefix);
    }

    auto stats = migrator.GetStats();
    std::cerr << "[Migrator] Done. Migrated " << migrated << " keys in "
              << stats.elapsed_seconds << " seconds.\n";

    migrator.Disconnect();
    g_migrator = nullptr;
    return 0;
}
