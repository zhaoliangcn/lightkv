#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/config.h"
#include <iostream>
#include <string>
#include <csignal>
#include <thread>

static lightkv::Server* g_server = nullptr;

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        std::cerr << "\n[LightKV] Received signal " << sig << ", shutting down...\n";
        if (g_server) g_server->Stop();
    }
}

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --db-path PATH        Database directory (default: ./lightkv_data)\n"
              << "  --config FILE         Load options from JSON config file\n"
              << "  --tcp-port PORT       TCP server port (default: 6379, 0=disable)\n"
              << "  --http-port PORT      HTTP monitor port (default: 8080, 0=disable)\n"
              << "  --tcp-host HOST       TCP bind address (default: 0.0.0.0)\n"
              << "  --http-host HOST      HTTP bind address (default: 0.0.0.0)\n"
              << "  --max-conns N         Max connections (default: 1024)\n"
              << "  --help                Show this help\n";
}

int main(int argc, char* argv[]) {
    lightkv::Options db_opts;
    lightkv::ServerOptions srv_opts;

    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--db-path" && i + 1 < argc) {
            db_opts.db_path = argv[++i];
        } else if (arg == "--config" && i + 1 < argc) {
            std::string config_file = argv[++i];
            auto s = lightkv::LoadOptionsFromFile(config_file, &db_opts);
            if (!s.ok()) {
                std::cerr << "[LightKV] Failed to load config: " << s.ToString() << "\n";
                return 1;
            }
            std::cerr << "[LightKV] Loaded config from " << config_file << "\n";
        } else if (arg == "--tcp-port" && i + 1 < argc) {
            uint16_t port = static_cast<uint16_t>(std::stoi(argv[++i]));
            if (port == 0) srv_opts.enable_tcp = false;
            else srv_opts.tcp_port = port;
        } else if (arg == "--http-port" && i + 1 < argc) {
            uint16_t port = static_cast<uint16_t>(std::stoi(argv[++i]));
            if (port == 0) srv_opts.enable_http = false;
            else srv_opts.http_port = port;
        } else if (arg == "--tcp-host" && i + 1 < argc) {
            srv_opts.tcp_host = argv[++i];
        } else if (arg == "--http-host" && i + 1 < argc) {
            srv_opts.http_host = argv[++i];
        } else if (arg == "--max-conns" && i + 1 < argc) {
            srv_opts.max_connections = std::stoi(argv[++i]);
        } else {
            std::cerr << "[LightKV] Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Signal handling
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Open database
    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(db_opts, &db);
    if (!s.ok()) {
        std::cerr << "[LightKV] Failed to open database: " << s.ToString() << "\n";
        return 1;
    }
    std::cerr << "[LightKV] Database opened at " << db_opts.db_path << "\n";

    // Create and run server
    lightkv::Server server(db, srv_opts);
    g_server = &server;

    std::cerr << "[LightKV] Server starting...\n";
    server.Run();

    // Cleanup
    delete db;
    std::cerr << "[LightKV] Server stopped.\n";
    return 0;
}
