// Cluster-mode (Raft) integration test.
//
// Regression context: in --cluster mode only SET and DEL were routed through the
// Raft log. Every other mutating command (HSET/LPUSH/ZADD/INCR/...) wrote
// straight to the local DB, while followers' RaftDBAdapter::Apply silently
// dropped them. The cluster therefore diverged with no error anywhere.
// Even SET with EX/PX was affected: the value was replicated but the TTL
// metadata key was not.
//
// The server must now REJECT writes it cannot replicate, so that "cluster
// diverges" becomes "feature unavailable".

#include "test_paths.h"
#include "lightkv/db.h"
#include "lightkv/server.h"
#include "lightkv/client.h"
#include "lightkv/raft.h"
#include "lightkv/state_snapshot.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

using namespace lightkv;

static int g_total = 0;
static int g_failed = 0;

#define CHECK(name, expr) do { \
    ++g_total; \
    if (!(expr)) { ++g_failed; std::cout << "  FAIL: " << name << std::endl; } \
    else { std::cout << "  PASS: " << name << std::endl; } \
} while (0)

// Mirrors RaftCommandAdapter in tools/server_main.cpp: applies the replicated
// command through the Server, so leader and follower share one code path.
class DBStateMachine : public RaftStateMachine {
public:
    explicit DBStateMachine(DB* db) : db_(db) {}
    void SetServer(Server* s) { server_ = s; }

    void Apply(const std::string& data, std::string* result) override {
        std::vector<std::string> args;
        if (!DecodeRaftCommand(data, &args) || args.empty()) return;
        if (server_) {
            std::string resp = server_->ExecuteLocally(args);
            if (result) *result = resp;
            return;
        }
        if (args.size() < 3) return;
        WriteOptions wo;
        if (args[0] == "SET") {
            db_->Put(wo, args[1], args[2]);
            if (result) *result = "+OK\r\n";
        } else if (args[0] == "DEL") {
            db_->Delete(wo, args[1]);
            if (result) *result = ":1\r\n";
        }
    }
    // Snapshot payload format and restore live in the library, so the test
// exercises the same code the server runs instead of a copy of it.
std::string TakeSnapshot() override {
    std::string image;
    if (!EncodeStateImage(db_, &image).ok()) return "";
    return image;
}
Status ApplySnapshot(const std::string& data) override {
    return RestoreStateImage(db_, data);
}

private:
    DB* db_;
    Server* server_ = nullptr;
};

// Loopback RPC for a one-node cluster: the node talks to itself.
class SelfRPC : public RaftRPC {
public:
    Raft* self = nullptr;
    AppendEntriesResponse SendAppendEntries(uint64_t, const AppendEntriesRequest& req) override {
        return self ? self->HandleAppendEntries(req) : AppendEntriesResponse{};
    }
    RequestVoteResponse SendRequestVote(uint64_t, const RequestVoteRequest& req) override {
        return self ? self->HandleRequestVote(req) : RequestVoteResponse{};
    }
    InstallSnapshotResponse SendInstallSnapshot(uint64_t, const InstallSnapshotRequest&) override {
        return InstallSnapshotResponse{};
    }
};

int main() {
    const std::string db_path = LIGHTKV_TEST_TMP "/lightkv_cluster_test";
    lightkv_remove_tree(db_path);

    Options db_opts;
    db_opts.db_path = db_path;
    db_opts.create_if_missing = true;
    DB* db = nullptr;
    auto ds = DB::Open(db_opts, &db);
    if (!ds.ok()) {
        std::cerr << "Failed to open DB: " << ds.ToString() << std::endl;
        return 1;
    }

    RaftOptions raft_opts;
    raft_opts.node_id = 1;
    raft_opts.peers.push_back(RaftPeer{1, "127.0.0.1", 16390, true});
    raft_opts.enable_raft = true;
    raft_opts.election_timeout_min_ms = 150;
    raft_opts.election_timeout_max_ms = 300;
    raft_opts.heartbeat_interval_ms = 50;

    DBStateMachine sm(db);
    SelfRPC rpc;
    Raft raft(raft_opts, &sm, &rpc);
    rpc.self = &raft;
    auto rs = raft.Initialize(db_path);
    if (!rs.ok()) {
        std::cerr << "Failed to init Raft: " << rs.ToString() << std::endl;
        return 1;
    }
    raft.Start();

    // Wait for the single node to elect itself.
    for (int i = 0; i < 60 && raft.GetStats().role != RaftRole::kLeader; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    const bool is_leader = raft.GetStats().role == RaftRole::kLeader;
    if (!is_leader) {
        std::cerr << "Single-node Raft did not become leader (role="
                  << static_cast<int>(raft.GetStats().role) << ")\n";
        return 1;
    }
    std::cout << "[Cluster Test] single node elected leader\n" << std::endl;

    // A proposed entry must actually reach commit and be applied.
    // Regression: the voter tally double-counted the leader, so a single-node
    // cluster computed majority=2 with only one replica and NOTHING ever
    // committed — every ProposeSync() stalled for its full timeout and reported
    // failure even though the log entry was present.
    {
        // Becoming leader and committing the leader's no-op entry are not
        // atomic: the replication thread commits it on its next tick, so poll
        // rather than sampling once.
        auto wait_for_commit = [&raft](uint64_t idx, int max_ms) {
            for (int waited = 0; waited < max_ms; waited += 20) {
                if (raft.GetStats().commit_index >= idx) return true;
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            return raft.GetStats().commit_index >= idx;
        };

        CHECK("leader commits its no-op entry", wait_for_commit(1, 2000));

        const int64_t idx = raft.Propose("PROBE");
        CHECK("Propose returns a log index", idx > 0);

        const bool committed = idx > 0 && wait_for_commit(static_cast<uint64_t>(idx), 2000);
        auto a = raft.GetStats();
        if (!committed) {
            std::fprintf(stderr, "  [probe] entry %lld never committed (commit=%llu applied=%llu)\n",
                         (long long)idx, (unsigned long long)a.commit_index,
                         (unsigned long long)a.last_applied);
        }
        CHECK("a proposed entry reaches commit_index", committed);
        CHECK("a committed entry is applied to the state machine",
              idx > 0 && a.last_applied >= static_cast<uint64_t>(idx));
    }

    ServerOptions srv_opts;
    srv_opts.tcp_port = 16383;
    srv_opts.http_port = 18083;
    srv_opts.raft_ptr = &raft;
    Server server(db, srv_opts);
    sm.SetServer(&server);   // replicated commands run through the Server
    std::thread server_thread([&server]() { server.Run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    Client c;
    if (!c.Connect("127.0.0.1", 16383)) {
        std::cerr << "Failed to connect to server\n";
        return 1;
    }

    // Send an arbitrary command and return the raw RESP reply. Client's
    // send_command() is private, but the pipeline API carries arbitrary argv.
    auto send_raw = [&c](const std::string& line) -> std::string {
        std::vector<std::string> args;
        std::string cur;
        for (char ch : line) {
            if (ch == ' ') { if (!cur.empty()) { args.push_back(cur); cur.clear(); } }
            else { cur.push_back(ch); }
        }
        if (!cur.empty()) args.push_back(cur);
        if (args.empty()) return "<empty>";
        c.Pipeline();
        c.Queue(args);
        auto results = c.ExecPipeline();
        return results.empty() ? "<no reply>" : results[0];
    };

    // ── Replicated writes must still work ────────────────────────────────
    CHECK("SET is accepted (replicated)", send_raw("SET ck cv") == "+OK\r\n");
    CHECK("GET returns the replicated value", send_raw("GET ck") == "$2\r\ncv\r\n");
    CHECK("DEL is accepted (replicated)", send_raw("DEL ck") == ":1\r\n");
    CHECK("GET after DEL returns nil", send_raw("GET ck") == "$-1\r\n");

    // ── Deterministic writes that CAN be replicated must now be accepted ──
    // These used to be rejected outright (only SET/DEL were replicated).
    // Each command uses its own key so the checks do not depend on each other.
    auto is_err = [](const std::string& r) {
        return r.rfind("-ERR", 0) == 0 || r.rfind("-READONLY", 0) == 0 ||
               r.rfind("-NotFound", 0) == 0 || r.rfind("-WRONGTYPE", 0) == 0;
    };

    struct Allowed { const char* cmd; const char* expect; const char* label; };
    const Allowed kAllowed[] = {
        // HSET returns the number of NEW fields added (Redis semantics).
        {"HSET ch f v",      ":1\r\n",    "HSET"},
        {"HMSET chh a 1 b 2", "+OK\r\n",   "HMSET"},
        {"LPUSH ll a b c",   ":3\r\n",    "LPUSH"},
        {"LSET ll 0 z",      "+OK\r\n",   "LSET"},
        {"LREM ll 1 a",      ":1\r\n",    "LREM"},
        {"LPOP ll",          "$1\r\nz\r\n", "LPOP"},
        {"RPUSH rl a",       ":1\r\n",    "RPUSH"},
        {"RPOP rl",          "$1\r\na\r\n", "RPOP"},
        {"LTRIM rl 0 -1",    "+OK\r\n",   "LTRIM"},
        {"SADD cs m",        ":1\r\n",    "SADD"},
        {"SREM cs m",        ":1\r\n",    "SREM"},
        {"SMOVE cs cs m",    ":0\r\n",    "SMOVE"},
        {"ZADD cz 1 m",      ":1\r\n",    "ZADD"},
        {"ZREM cz m",        ":1\r\n",    "ZREM"},
        {"INCR ci",          ":1\r\n",    "INCR"},
        {"DECR ci",          ":0\r\n",    "DECR"},
        {"INCRBY ci 5",      ":5\r\n",    "INCRBY"},
        {"DECRBY ci 2",      ":3\r\n",    "DECRBY"},
        // INCRBYFLOAT returns a bulk string with trailing zeros trimmed
        // (Redis semantics), not std::to_string's 6 fixed decimals.
        {"INCRBYFLOAT ci 1.0", "$1\r\n4\r\n", "INCRBYFLOAT"},
        {"APPEND ck2 hello", ":5\r\n",    "APPEND"},
        {"SETNX ck3 v",      ":1\r\n",    "SETNX"},
        {"GETSET ck3 w",     "$1\r\nv\r\n", "GETSET"},
        {"HDEL ch f",        ":1\r\n",    "HDEL"},
        {"HINCRBY ch f2 1",  ":1\r\n",    "HINCRBY"},
        {"SETBIT cb 3 1",    ":0\r\n",    "SETBIT"},
        {"GEOADD cgeo 1 1 m", ":1\r\n",   "GEOADD"},
        // DELRANGE reports how many live keys the tombstone shadowed.
        {"SET dr1 a",          "+OK\r\n",   "SET for DELRANGE 1"},
        {"SET dr2 b",          "+OK\r\n",   "SET for DELRANGE 2"},
        {"DELRANGE dr1 dr9",  ":2\r\n",    "DELRANGE counts deleted keys"},
        {"DELRANGE dr1 dr9",  ":0\r\n",    "DELRANGE on an already-empty range"},
        {"DELRANGE zzz1 zzz2", ":0\r\n",   "DELRANGE matching nothing"},
        {"MSET m1 v1 m2 v2", "+OK\r\n",   "MSET"},
        {"SET rk v",         "+OK\r\n",   "SET for RENAME"},
        {"RENAME rk rk2",    "+OK\r\n",   "RENAME"},
    };
    for (const auto& tc : kAllowed) {
        std::string r = send_raw(tc.cmd);
        if (r != tc.expect) {
            std::fprintf(stderr, "    %s -> '%s', expected '%s'\n",
                         tc.cmd, r.c_str(), tc.expect);
        }
        CHECK(std::string(tc.label) + " is replicated and accepted", r == tc.expect);
    }

    // A replicated command that fails on data grounds still round-trips the
    // apply's own response rather than being silently retried.
    CHECK("replicated command reports the apply's error",
          is_err(send_raw("RENAME never_existed other")));

    // The replicated writes must have reached the state machine (i.e. the DB),
    // not merely been accepted at the dispatch layer. Hash fields are stored
    // under the "\x02_hash_<name>:<field>" prefix.
    {
        ReadOptions ro;
        std::string v;
        auto st = db->Get(ro, "\x02_hash_chh:a", &v);
        if (!st.ok()) std::fprintf(stderr, "    replicated HMSET did not reach the DB\n");
        CHECK("replicated HMSET reached the state machine", st.ok());
    }

    // ── Writes that CANNOT be replicated must still be rejected ──────────
    struct Case { const char* cmd; const char* label; };
    const Case kRejected[] = {
        // non-deterministic: wall clock / randomness
        {"EXPIRE ck 10",       "EXPIRE"},
        {"PEXPIRE ck 10000",   "PEXPIRE"},
        {"EXPIREAT ck 9999999999", "EXPIREAT"},
        {"SETEX ck7 10 v",     "SETEX"},
        {"PSETEX ck7 10000 v", "PSETEX"},
        {"SET ck8 v EX 100",   "SET with EX"},
        {"SET ck9 v PX 100",   "SET with PX"},
        {"SET cka v KEEPTTL",  "SET with KEEPTTL"},
        {"PERSIST ck6",        "PERSIST"},
        {"SPOP cs2",           "SPOP"},
        // deterministic but not wired into the replicated command set
        {"HSETNX ch2 f v",     "HSETNX"},
        {"HINCRBYFLOAT ch f 1.5", "HINCRBYFLOAT"},
        {"SETRANGE cstr 1 ab", "SETRANGE"},
        {"LPUSHX cl w",        "LPUSHX"},
        {"RPUSHX cl2 w",       "RPUSHX"},
        {"LINSERT cl BEFORE w x", "LINSERT"},
        {"RPOPLPUSH cl cl2",   "RPOPLPUSH"},
        {"BITOP AND dst s1",   "BITOP"},
        {"PFADD chy m",        "PFADD"},
        {"ZINCRBY cz 2 m",     "ZINCRBY"},
        {"ZREMRANGEBYSCORE cz 0 5", "ZREMRANGEBYSCORE"},
        {"SINTERSTORE d1 cs cs2", "SINTERSTORE"},
        {"FLUSHALL",           "FLUSHALL"},
        {"FLUSHDB",            "FLUSHDB"},
        // These two used to be listed in is_raft_replicated_command with no
        // handler behind them: they cleared every guard, were COMMITTED to the
        // Raft log, and only then failed in ExecuteLocally with "unknown
        // command". The client saw an error for an entry that was already
        // durably committed.
        {"UNLINK ck2",         "UNLINK"},
        {"HSETNX ch3 f v",     "HSETNX (regression)"},
    };
    for (const auto& tc : kRejected) {
        std::string r = send_raw(tc.cmd);
        bool ok = r.rfind("-ERR", 0) == 0 || r.rfind("-READONLY", 0) == 0;
        if (!ok) {
            std::fprintf(stderr, "    %s -> %s\n", tc.cmd, r.c_str());
        }
        CHECK(std::string(tc.label) + " is rejected in cluster mode", ok);
    }

    // A rejection must come from the write guard -- "cannot be replicated" --
    // and not from the apply stage. "unknown command" means the entry was
    // already committed and only then failed, which is the F-13/F-26 failure
    // shape: the client retries, and the retried command can double-apply
    // whatever was logged after it.
    for (const char* cmd : {"UNLINK ck2", "HSETNX ch3 f v"}) {
        std::string r = send_raw(cmd);
        bool guarded = r.find("not supported in cluster mode") != std::string::npos;
        if (!guarded) {
            std::fprintf(stderr,
                         "    %s was rejected too late (after commit): %s\n", cmd,
                         r.c_str());
        }
        CHECK(std::string(cmd) + " is refused by the write guard, not after commit",
              guarded);
    }

    // ── Snapshot image round-trip ────────────────────────────────────
    // The leader and a fresh follower must end up with byte-identical state.
    // Range tombstones are deliberately not part of the image; after Clear()
    // there is nothing left for them to shadow.
    {
        // Own the key for this block: the read-only section below has not run
        // yet, so nothing else in the test guarantees it exists.
        CHECK("SET for the snapshot block succeeds",
              send_raw("SET snapkey snapvalue") == "+OK\r\n");

        std::string image;
        Status es = EncodeStateImage(db, &image);
        CHECK("EncodeStateImage succeeds", es.ok());
        CHECK("snapshot image is non-empty", !image.empty());

        std::vector<std::pair<std::string, std::string>> decoded;
        Status ds = DecodeStateImage(image, &decoded);
        CHECK("DecodeStateImage round-trips", ds.ok() && !decoded.empty());

        const std::string follower_path =
            std::string(LIGHTKV_TEST_TMP) + "/lightkv_cluster_follower";
        lightkv_remove_tree(follower_path);
        Options follower_opts;
        follower_opts.db_path = follower_path;
        follower_opts.create_if_missing = true;
        DB* follower = nullptr;
        CHECK("follower DB opens", DB::Open(follower_opts, &follower).ok());

        // Give the follower divergent state first, so the install has to wipe it.
        WriteOptions fwo;
        follower->Put(fwo, "stale_key", "stale_value");

        Status rs = RestoreStateImage(follower, image);
        CHECK("RestoreStateImage succeeds", rs.ok());

        ReadOptions fro;
        std::string value;
        CHECK("follower adopted the leader's key",
              follower->Get(fro, "snapkey", &value).ok() && value == "snapvalue");
        CHECK("follower dropped its own stale key", !follower->Exists(fro, "stale_key"));

        // Every key on the leader must be present with the same value.
        bool identical = true;
        for (const auto& row : decoded) {
            std::string fv;
            Status st = follower->Get(fro, row.first, &fv);
            if (!st.ok() || fv != row.second) {
                std::fprintf(stderr, "    follower differs at '%s'\n", row.first.c_str());
                identical = false;
                break;
            }
        }
        CHECK("follower state is identical to the leader's", identical);

        // A corrupt payload must be refused WITHOUT destroying what the node
        // already has.
        Status before_corrupt = follower->Get(fro, "snapkey", &value);
        std::string truncated = image.substr(0, image.size() / 2);
        CHECK("truncated image is rejected", !RestoreStateImage(follower, truncated).ok());
        CHECK("node still serves its state after a rejected restore",
              before_corrupt.ok() && follower->Get(fro, "snapkey", &value).ok() &&
                  value == "snapvalue");

        std::string garbage(64, 'X');
        CHECK("garbage image is rejected", !RestoreStateImage(follower, garbage).ok());

        delete follower;
        lightkv_remove_tree(follower_path);
    }

    // ── Snapshot image over a large key space ───────────────────────────
    // Regression: an earlier version paged with DB::Scan using a batch limit.
    // Scan has no cursor -- every call restarts at SeekToFirst() and appends --
    // so that loop re-emitted the same prefix forever and never terminated when
    // the key count was an exact multiple of the batch size. The key count below
    // is a round number precisely to catch that.
    {
        const std::string big_path =
            std::string(LIGHTKV_TEST_TMP) + "/lightkv_cluster_bigsnap";
        lightkv_remove_tree(big_path);
        Options big_opts;
        big_opts.db_path = big_path;
        big_opts.create_if_missing = true;
        DB* big = nullptr;
        CHECK("large snapshot DB opens", DB::Open(big_opts, &big).ok());

        constexpr int kKeys = 20000;
        WriteOptions bwo;
        for (int i = 0; i < kKeys; ++i) {
            big->Put(bwo, "bk" + std::to_string(i), "bv" + std::to_string(i));
        }

        std::string image;
        CHECK("large EncodeStateImage succeeds", EncodeStateImage(big, &image).ok());

        std::vector<std::pair<std::string, std::string>> rows;
        CHECK("large image decodes", DecodeStateImage(image, &rows).ok());
        if (static_cast<int>(rows.size()) != kKeys) {
            std::fprintf(stderr, "    image holds %zu rows, expected %d\n",
                         rows.size(), kKeys);
        }
        CHECK("image holds every key exactly once",
              static_cast<int>(rows.size()) == kKeys);

        // Spot-check the ends, which is where a truncation would show up.
        std::string v0, vN;
        ReadOptions bro;
        big->Get(bro, "bk0", &v0);
        big->Get(bro, "bk" + std::to_string(kKeys - 1), &vN);
        bool found_ends = false;
        for (const auto& row : rows) {
            if (row.first == "bk0") found_ends = row.second == "bv0";
            if (row.first == "bk" + std::to_string(kKeys - 1))
                found_ends = found_ends && row.second == "bv" + std::to_string(kKeys - 1);
        }
        CHECK("image contains the first and last key", found_ends);

        delete big;
        lightkv_remove_tree(big_path);
    }

    // ── Read-only commands must be unaffected ───────────────────────────
    send_raw("SET ro_before 1");
    CHECK("GET still works after rejections", send_raw("GET ro_before") == "$1\r\n1\r\n");
    CHECK("EXISTS still works", send_raw("EXISTS ro_before") == ":1\r\n");
    CHECK("PING still works", send_raw("PING") == "+PONG\r\n");
    CHECK("unknown command is not masked by the guard",
          send_raw("NOSUCHCMD").find("unknown command") != std::string::npos);

    // ── The rejected writes must not have touched the DB ────────────────
    {
        ReadOptions ro;
        std::string v;
        Status st = db->Get(ro, "ch", &v);
        bool absent = st.IsNotFound();
        if (!absent) std::fprintf(stderr, "    HSET leaked into DB: %s\n", v.c_str());
        CHECK("rejected HSET did not modify the local DB", absent);
    }

    c.Disconnect();
    server.Stop();
    if (server_thread.joinable()) server_thread.join();
    raft.Stop();
    delete db;
    lightkv_remove_tree(db_path);

    std::cout << "\nCluster Test Results: " << (g_total - g_failed) << "/"
              << g_total << " passed" << std::endl;
    if (g_failed) {
        std::cout << "FAILED" << std::endl;
        return 1;
    }
    std::cout << "All cluster tests passed!" << std::endl;
    return 0;
}
