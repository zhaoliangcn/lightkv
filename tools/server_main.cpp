#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/config.h"
#include "lightkv/raft.h"
#include "lightkv/raft_server.h"
#include <iostream>
#include <string>
#include <csignal>
#include <thread>
#include <vector>
#include <sstream>

static lightkv::Server* g_server = nullptr;
static lightkv::Raft* g_raft = nullptr;
static lightkv::RaftServer* g_raft_server = nullptr;

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        std::cerr << "\n[LightKV] Received signal " << sig << ", shutting down...\n";
        if (g_raft_server) {
            g_raft_server->Stop();
            g_raft_server = nullptr;
        }
        if (g_raft) {
            g_raft->Stop();
            delete g_raft;
            g_raft = nullptr;
        }
        if (g_server) g_server->Stop();
    }
}

// 解析 raft_peers_config 字符串为 RaftPeer 列表
// 格式: "id1:host1:port1:is_voter,id2:host2:port2:is_voter"
static std::vector<lightkv::RaftPeer> parse_raft_peers(const std::string& config) {
    std::vector<lightkv::RaftPeer> peers;
    if (config.empty()) return peers;

    std::stringstream ss(config);
    std::string segment;
    while (std::getline(ss, segment, ',')) {
        if (segment.empty()) continue;
        std::stringstream seg_ss(segment);
        std::string part;
        std::vector<std::string> parts;
        while (std::getline(seg_ss, part, ':')) {
            parts.push_back(part);
        }
        if (parts.size() >= 4) {
            lightkv::RaftPeer peer;
            peer.id = static_cast<uint64_t>(std::stoull(parts[0]));
            peer.host = parts[1];
            peer.port = static_cast<uint16_t>(std::stoi(parts[2]));
            peer.is_voter = (parts[3] == "1");
            peers.push_back(peer);
        }
    }
    return peers;
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
              << "  --worker-threads N    Worker thread count (default: 0 = single-threaded)\n"
              << "  --requirepass PWD     Require password authentication (default: none)\n"
              << "  --master-host HOST    Replication: connect to this Master (default: none = act as Master)\n"
              << "  --master-port PORT    Replication: Master port (default: 6379)\n"
              << "  --master-auth PWD     Replication: Master authentication password\n"
              << "  --readonly            Replication: enable read-only mode\n"
              << "  --ttl-scan-ms N       TTL active scan interval in ms (default: 1000, 0=disable)\n"
              << "  --ttl-sample N        TTL sample count per scan (default: 20)\n"
              << "  --cluster             Enable Raft cluster mode (分布式集群模式)\n"
              << "  --node-id N           Raft node ID (default: 0)\n"
              << "  --raft-host HOST      Raft internal communication host (default: 0.0.0.0)\n"
              << "  --raft-port PORT      Raft internal communication port (default: 16379)\n"
              << "  --raft-peers LIST     Raft peers config (format: id:host:port:is_voter,...)\n"
              << "  --election-timeout N  Raft election timeout min ms (default: 150)\n"
              << "  --heartbeat-interval N Raft heartbeat interval ms (default: 50)\n"
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
        } else if (arg == "--worker-threads" && i + 1 < argc) {
            srv_opts.worker_threads = std::stoi(argv[++i]);
        } else if (arg == "--requirepass" && i + 1 < argc) {
            srv_opts.requirepass = argv[++i];
        } else if (arg == "--master-host" && i + 1 < argc) {
            srv_opts.master_host = argv[++i];
        } else if (arg == "--master-port" && i + 1 < argc) {
            srv_opts.master_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--master-auth" && i + 1 < argc) {
            srv_opts.master_auth = argv[++i];
        } else if (arg == "--readonly" && i + 1 < argc) {
            srv_opts.readonly = true;
        } else if (arg == "--ttl-scan-ms" && i + 1 < argc) {
            srv_opts.ttl_scan_interval_ms = std::stoi(argv[++i]);
        } else if (arg == "--ttl-sample" && i + 1 < argc) {
            srv_opts.ttl_sample_count = std::stoi(argv[++i]);
        } else if (arg == "--cluster") {
            db_opts.enable_raft = true;
        } else if (arg == "--node-id" && i + 1 < argc) {
            db_opts.raft_node_id = static_cast<uint64_t>(std::stoull(argv[++i]));
        } else if (arg == "--raft-host" && i + 1 < argc) {
            db_opts.raft_host = argv[++i];
        } else if (arg == "--raft-port" && i + 1 < argc) {
            db_opts.raft_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--raft-peers" && i + 1 < argc) {
            db_opts.raft_peers_config = argv[++i];
        } else if (arg == "--election-timeout" && i + 1 < argc) {
            db_opts.raft_election_timeout_min_ms = std::stoi(argv[++i]);
            db_opts.raft_election_timeout_max_ms = db_opts.raft_election_timeout_min_ms * 2;
        } else if (arg == "--heartbeat-interval" && i + 1 < argc) {
            db_opts.raft_heartbeat_interval_ms = std::stoi(argv[++i]);
        } else {
            std::cerr << "[LightKV] Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Signal handling
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Raft 状态机适配器：将 Raft 命令应用到 DB
    class RaftDBAdapter : public lightkv::RaftStateMachine {
    public:
        explicit RaftDBAdapter(lightkv::DB* db) : db_(db) {}

        void Apply(const std::string& command_data) override {
            // 命令格式：第一个字节是操作类型，后面是 key 和 value
            // v0 简化版：仅支持 Put 操作
            // 格式: "P|key|value" 或 "D|key|" 或 "B|count|op1|op2|..."
            if (command_data.empty() || command_data.size() < 2) return;

            char type = command_data[0];
            if (type == 'P') {
                // Put: "P|{key_len}:{key}|{value_len}:{value}" 或 "P|key|value"
                size_t sep1 = command_data.find('|', 2);
                if (sep1 == std::string::npos) return;
                std::string key = command_data.substr(2, sep1 - 2);
                std::string value = command_data.substr(sep1 + 1);
                lightkv::WriteOptions wopts;
                wopts.sync = false;
                db_->Put(wopts, key, value);
            }
            // Delete / Batch 等操作后续扩展
        }

        std::string TakeSnapshot() override {
            // 简化：暂不实现快照
            return "";
        }

        void ApplySnapshot(const std::string& data) override {
            // 简化：暂不实现
            (void)data;
        }

    private:
        lightkv::DB* db_;
    };

    // Open database
    lightkv::DB* db = nullptr;
    auto s = lightkv::DB::Open(db_opts, &db);
    if (!s.ok()) {
        std::cerr << "[LightKV] Failed to open database: " << s.ToString() << "\n";
        return 1;
    }
    std::cerr << "[LightKV] Database opened at " << db_opts.db_path << "\n";

    // 初始化 Raft 集群模式
    lightkv::Raft* raft = nullptr;
    lightkv::RaftServer* raft_server = nullptr;
    RaftDBAdapter* adapter = nullptr;

    if (db_opts.enable_raft) {
        std::cerr << "[LightKV] Starting in Raft cluster mode (node_id="
                  << db_opts.raft_node_id << ")...\n";

        // 解析集群节点列表
        auto peers = parse_raft_peers(db_opts.raft_peers_config);
        if (peers.empty()) {
            std::cerr << "[LightKV] ERROR: --raft-peers must be specified in cluster mode\n";
            delete db;
            return 1;
        }

        // 创建 Raft 配置
        lightkv::RaftOptions raft_opts;
        raft_opts.node_id = db_opts.raft_node_id;
        raft_opts.peers = peers;
        raft_opts.enable_raft = true;
        raft_opts.election_timeout_min_ms = db_opts.raft_election_timeout_min_ms;
        raft_opts.election_timeout_max_ms = db_opts.raft_election_timeout_max_ms;
        raft_opts.heartbeat_interval_ms = db_opts.raft_heartbeat_interval_ms;

        // 创建 Raft 状态机适配器
        adapter = new RaftDBAdapter(db);

        // 创建 Raft 引擎
        raft = new lightkv::Raft(raft_opts, adapter, nullptr);  // RPC 在 RaftServer 创建后设置

        auto raft_status = raft->Initialize(db_opts.db_path);
        if (!raft_status.ok()) {
            std::cerr << "[LightKV] Raft init failed: " << raft_status.ToString() << "\n";
            delete adapter;
            delete raft;
            delete db;
            return 1;
        }
        std::cerr << "[LightKV] Raft engine initialized\n";

        // 创建 Raft 网络层，传入 Raft 引擎
        raft_server = new lightkv::RaftServer(
            db_opts.raft_host, db_opts.raft_port, raft);

        raft_server->Initialize(peers);

        // 设置 RPC 回调 — 必须在 Raft::Start() 之前
        raft->SetRPC(raft_server);

        raft_server->Start();
        raft->Start();
        std::cerr << "[LightKV] Raft cluster started on "
                  << db_opts.raft_host << ":" << db_opts.raft_port << "\n";

        g_raft = raft;
        g_raft_server = raft_server;
    }

    // 传递集群配置到 ServerOptions（供 CLUSTER 命令使用）
    if (db_opts.enable_raft) {
        srv_opts.enable_cluster = true;
        srv_opts.cluster_node_id = db_opts.raft_node_id;
        srv_opts.cluster_peers_config = db_opts.raft_peers_config;
        srv_opts.cluster_raft_port = db_opts.raft_port;
    }

    // Create and run server
    lightkv::Server server(db, srv_opts);
    g_server = &server;

    std::cerr << "[LightKV] Server starting...\n";
    server.Run();

    // Cleanup
    if (raft_server) {
        raft_server->Stop();
        delete raft_server;
    }
    if (raft) {
        raft->Stop();
        delete raft;
    }
    if (adapter) delete adapter;
    delete db;
    std::cerr << "[LightKV] Server stopped.\n";
    return 0;
}
