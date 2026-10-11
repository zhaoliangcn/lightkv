// Raft snapshot tests.
//
// Focus: the snapshot/compaction path must never destroy committed log entries
// that are the only remaining copy of the state.
//
// Regression context: TriggerSnapshot() used to substitute a placeholder string
// ("snapshot_at_<index>") whenever the state machine returned an empty snapshot,
// and then compacted the log anyway. Any follower that fell behind could no
// longer be caught up, and InstallSnapshot would hand it a placeholder instead
// of real state.

#include "test_paths.h"
#include "lightkv/raft.h"
#include <cassert>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace lightkv;

// 鈹€鈹€鈹€ Test doubles 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

// State machine that cannot produce a snapshot (returns empty), which is what
// RaftDBAdapter does today.
class NullSnapshotSM : public RaftStateMachine {
public:
    void Apply(const std::string&, std::string*) override { ++applied_; }
    std::string TakeSnapshot() override { return ""; }
    Status ApplySnapshot(const std::string&) override {
        ++applied_snapshots_;
        return Status::OK();
    }

    int applied_ = 0;
    int applied_snapshots_ = 0;
};

// State machine that produces a real snapshot.
class WorkingSnapshotSM : public RaftStateMachine {
public:
    void Apply(const std::string&, std::string*) override {}
    std::string TakeSnapshot() override { return std::string("SNAP") + std::to_string(++takes_); }
    Status ApplySnapshot(const std::string& data) override {
        last_ = data;
        return Status::OK();
    }

    int takes_ = 0;
    std::string last_;
};

// RPC stub — these tests never run the replication loop, so nothing is sent.
class NullRPC : public RaftRPC {
public:
    AppendEntriesResponse SendAppendEntries(uint64_t, const AppendEntriesRequest&) override { return {}; }
    RequestVoteResponse SendRequestVote(uint64_t, const RequestVoteRequest&) override { return {}; }
    InstallSnapshotResponse SendInstallSnapshot(uint64_t, const InstallSnapshotRequest&) override { return {}; }
    InstallSnapshotChunkResponse SendInstallSnapshotChunk(
        uint64_t, const InstallSnapshotChunkRequest&) override {
        return {};
    }
};

// Routes RPCs to in-process peer nodes. A node that is absent from the table is
// treated as unreachable, which is how the lagging-follower test simulates a
// node that was offline while the leader compacted its log.
//
// The peer is looked up under the lock and called OUTSIDE it: node A holds its
// own mu_ while it calls into here, and this then calls node B which takes B's
// mu_. Holding the table lock across that call inverts the order against A's
// thread and deadlocks the cluster.
class RoutingRPC : public RaftRPC {
public:
    void Add(uint64_t id, std::shared_ptr<Raft> node) {
        std::lock_guard<std::mutex> lock(mu_);
        nodes_[id] = std::move(node);
    }
    void Remove(uint64_t id) {
        std::lock_guard<std::mutex> lock(mu_);
        nodes_.erase(id);
    }
    void ResetCounters() {
        std::lock_guard<std::mutex> lock(mu_);
        snapshots_sent_ = 0;
    }
    int snapshots_sent() {
        std::lock_guard<std::mutex> lock(mu_);
        return snapshots_sent_;
    }

    AppendEntriesResponse SendAppendEntries(uint64_t peer_id,
                                           const AppendEntriesRequest& req) override {
        auto node = Lookup(peer_id);
        if (!node) return AppendEntriesResponse{};
        return node->HandleAppendEntries(req);
    }
    RequestVoteResponse SendRequestVote(uint64_t peer_id,
                                        const RequestVoteRequest& req) override {
        auto node = Lookup(peer_id);
        if (!node) return RequestVoteResponse{};
        return node->HandleRequestVote(req);
    }
    InstallSnapshotResponse SendInstallSnapshot(uint64_t peer_id,
                                                const InstallSnapshotRequest& req) override {
        {
            std::lock_guard<std::mutex> lock(mu_);
            ++snapshots_sent_;
        }
        auto node = Lookup(peer_id);
        if (!node) return InstallSnapshotResponse{};
        return node->HandleInstallSnapshot(req);
    }

    InstallSnapshotChunkResponse SendInstallSnapshotChunk(
        uint64_t peer_id, const InstallSnapshotChunkRequest& req) override {
        {
            std::lock_guard<std::mutex> lock(mu_);
            ++chunks_sent_;
            last_chunk_ = req;
        }
        auto node = Lookup(peer_id);
        if (!node) return InstallSnapshotChunkResponse{};
        return node->HandleInstallSnapshotChunk(req);
    }

private:
    std::shared_ptr<Raft> Lookup(uint64_t id) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = nodes_.find(id);
        return it == nodes_.end() ? nullptr : it->second;
    }

    std::mutex mu_;
    std::map<uint64_t, std::shared_ptr<Raft>> nodes_;
    int snapshots_sent_ = 0;
    int chunks_sent_ = 0;
    InstallSnapshotChunkRequest last_chunk_;
    int chunk_cuts() {
        std::lock_guard<std::mutex> lock(mu_);
        return chunks_sent_;
    }
    InstallSnapshotChunkRequest last_chunk() {
        std::lock_guard<std::mutex> lock(mu_);
        return last_chunk_;
    }
    void reset_chunks() {
        std::lock_guard<std::mutex> lock(mu_);
        chunks_sent_ = 0;
        last_chunk_ = InstallSnapshotChunkRequest{};
    }
};

// State machine that records how many snapshots it installed, so a redundant
// InstallSnapshot can be told apart from a real one. fail_install_ makes the
// install fail on demand.
class CountingSnapshotSM : public RaftStateMachine {
public:
    void Apply(const std::string&, std::string*) override { ++applied_; }
    std::string TakeSnapshot() override { return "STATE-IMAGE"; }
    Status ApplySnapshot(const std::string& data) override {
        ++applied_snapshots_;
        last_ = data;
        if (fail_install_) return Status::Corruption("install failed on purpose");
        return Status::OK();
    }

    int applied_ = 0;
    int applied_snapshots_ = 0;
    bool fail_install_ = false;
    std::string last_;
};

static RaftOptions MakeSingleNodeOptions() {
    RaftOptions opts;
    opts.node_id = 1;
    opts.peers.push_back(RaftPeer{1, "127.0.0.1", 16379, true});
    opts.enable_raft = true;
    // Long election timeout: these tests drive the log directly and must not be
    // disturbed by an election changing the term underneath them.
    opts.election_timeout_min_ms = 60000;
    opts.election_timeout_max_ms = 120000;
    opts.heartbeat_interval_ms = 50;
    return opts;
}

// Build a Raft node that has applied N entries, so TriggerSnapshot() has
// something to do. Entries are pushed in through HandleAppendEntries + a manual
// commit, which is what the replication path does.
static void SeedAppliedEntries(Raft* raft, int n) {
    // Value-initialize: AppendEntriesRequest has no default member initializers,
    // so plain `AppendEntriesRequest req;` leaves prev_log_index as garbage and
    // the request gets rejected on the prev-log consistency check.
    AppendEntriesRequest req{};
    // Use a fresh term so the request is never rejected as stale.
    req.term = raft->GetStats().current_term + 1;
    req.leader_id = 1;
    for (int i = 1; i <= n; ++i) {
        RaftLogEntry e;
        e.index = static_cast<uint64_t>(i);
        e.term = 1;
        e.type = RaftEntryType::kCommand;
        e.data = "cmd" + std::to_string(i);
        req.entries.push_back(e);
    }
    req.leader_commit = static_cast<uint64_t>(n);
    auto resp = raft->HandleAppendEntries(req);
    auto st = raft->GetStats();
    std::fprintf(stderr,
                 "  [raft] seed: req_term=%llu resp_term=%llu success=%d "
                 "log_count=%llu commit=%llu applied=%llu entries=%zu\n",
                 (unsigned long long)req.term, (unsigned long long)resp.term,
                 (int)resp.success, (unsigned long long)st.log_count,
                 (unsigned long long)st.commit_index,
                 (unsigned long long)st.last_applied, req.entries.size());
}

// 鈹€鈹€鈹€ Tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

// A state machine that cannot snapshot must cause TriggerSnapshot() to fail and
// must NOT compact the log.
void TestSnapshotRefusedWhenStateMachineCannotSnapshot() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_snapshot_null";
    lightkv_remove_tree(dir);

    NullSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    auto s = raft.Initialize(dir);
    assert(s.ok());

    SeedAppliedEntries(&raft, 50);
    const uint64_t before = raft.GetStats().log_count;
    std::fprintf(stderr, "  [raft] log entries before snapshot attempt: %llu\n",
                 (unsigned long long)before);
    assert(before > 1);

    const bool ok = raft.TriggerSnapshot();
    if (ok) {
        std::fprintf(stderr, "  TriggerSnapshot() succeeded despite an empty snapshot\n");
    }
    assert(!ok);

    const uint64_t after = raft.GetStats().log_count;
    std::fprintf(stderr, "  [raft] log entries after refused snapshot:  %llu\n",
                 (unsigned long long)after);
    if (after != before) {
        std::fprintf(stderr, "  log was compacted despite a refused snapshot (%llu -> %llu)\n",
                     (unsigned long long)before, (unsigned long long)after);
    }
    assert(after == before);
    assert(raft.GetLastSnapshotIndex() == 0);
    assert(raft.GetSnapshotData().empty());

    raft.Stop();
    lightkv_remove_tree(dir);
}

// A working state machine must still be able to snapshot and compact 鈥?the fix
// must not disable legitimate snapshots.
void TestSnapshotCompactsWhenStateMachineWorks() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_snapshot_ok";
    lightkv_remove_tree(dir);

    WorkingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    auto s = raft.Initialize(dir);
    assert(s.ok());

    SeedAppliedEntries(&raft, 50);
    const uint64_t before = raft.GetStats().log_count;
    assert(before > 1);

    const bool ok = raft.TriggerSnapshot();
    if (!ok) {
        std::fprintf(stderr, "  TriggerSnapshot() failed with a working state machine\n");
    }
    assert(ok);

    const uint64_t after = raft.GetStats().log_count;
    std::fprintf(stderr, "  [raft] log entries %llu -> %llu after real snapshot\n",
                 (unsigned long long)before, (unsigned long long)after);
    assert(after < before);
    assert(!raft.GetSnapshotData().empty());

    raft.Stop();
    lightkv_remove_tree(dir);
}

// An empty InstallSnapshot payload must be rejected rather than truncating the
// follower's log without replacing its state.
void TestEmptyInstallSnapshotRejected() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_install_empty";
    lightkv_remove_tree(dir);

    NullSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    auto s = raft.Initialize(dir);
    assert(s.ok());

    SeedAppliedEntries(&raft, 20);
    const uint64_t before = raft.GetStats().log_count;

    InstallSnapshotRequest req{};
    req.term = 2;
    req.leader_id = 1;
    req.last_included_index = 5;
    req.last_included_term = 1;
    req.data = "";  // nothing real to install

    auto resp = raft.HandleInstallSnapshot(req);
    if (resp.success) {
        std::fprintf(stderr, "  empty InstallSnapshot was accepted\n");
    }
    assert(!resp.success);

    const uint64_t after = raft.GetStats().log_count;
    if (after != before) {
        std::fprintf(stderr, "  log changed on a rejected InstallSnapshot (%llu -> %llu)\n",
                     (unsigned long long)before, (unsigned long long)after);
    }
    assert(after == before);
    assert(sm.applied_snapshots_ == 0);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// A snapshot that is never persisted is lost on restart, leaving the leader with
// no baseline: it could neither ship a snapshot to a lagging follower nor safely
// compact again. SavePersistentState() used to hardcode the snapshot fields to 0.
void TestSnapshotSurvivesRestart() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_snapshot_restart";
    lightkv_remove_tree(dir);

    std::string payload;
    uint64_t snap_index = 0;
    {
        WorkingSnapshotSM sm;
        NullRPC rpc;
        Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
        assert(raft.Initialize(dir).ok());

        SeedAppliedEntries(&raft, 50);
        assert(raft.TriggerSnapshot());
        snap_index = raft.GetLastSnapshotIndex();
        payload = raft.GetSnapshotData();
        assert(snap_index > 0);
        assert(!payload.empty());
        raft.Stop();
    }

    // Fresh node, same directory: only what reached disk can come back.
    WorkingSnapshotSM sm2;
    NullRPC rpc2;
    Raft raft2(MakeSingleNodeOptions(), &sm2, &rpc2);
    assert(raft2.Initialize(dir).ok());

    const uint64_t restored_index = raft2.GetLastSnapshotIndex();
    std::fprintf(stderr,
                 "  [raft] snapshot baseline %llu -> %llu after restart, payload=%zu bytes\n",
                 (unsigned long long)snap_index, (unsigned long long)restored_index,
                 raft2.GetSnapshotData().size());
    if (restored_index != snap_index) {
        std::fprintf(stderr, "  snapshot baseline lost across restart\n");
    }
    assert(restored_index == snap_index);
    if (raft2.GetSnapshotData() != payload) {
        std::fprintf(stderr, "  snapshot payload lost or altered across restart\n");
    }
    assert(raft2.GetSnapshotData() == payload);

    raft2.Stop();
    lightkv_remove_tree(dir);
}

// Installing a snapshot must leave the node with a consistent baseline, and
// replaying the same snapshot must not rewind the state machine.
void TestInstallSnapshotRecordsBaselineAndIsIdempotent() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_install_baseline";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    InstallSnapshotRequest req{};
    req.term = 2;
    req.leader_id = 1;
    req.last_included_index = 40;
    req.last_included_term = 7;
    req.data = "STATE-IMAGE";

    auto resp = raft.HandleInstallSnapshot(req);
    if (!resp.success) {
        std::fprintf(stderr, "  a well-formed InstallSnapshot was rejected\n");
    }
    assert(resp.success);
    assert(sm.applied_snapshots_ == 1);
    assert(sm.last_ == "STATE-IMAGE");

    // The node now owns this baseline; without it a later compaction or a
    // promotion to leader would produce a snapshot inconsistent with its log.
    if (raft.GetLastSnapshotIndex() != 40 || raft.GetLastSnapshotTerm() != 7) {
        std::fprintf(stderr, "  InstallSnapshot did not record the baseline "
                             "(index=%llu term=%llu)\n",
                     (unsigned long long)raft.GetLastSnapshotIndex(),
                     (unsigned long long)raft.GetLastSnapshotTerm());
    }
    assert(raft.GetLastSnapshotIndex() == 40);
    assert(raft.GetLastSnapshotTerm() == 7);
    assert(raft.GetStats().last_applied == 40);

    // Raft retransmits an acked snapshot when a response is lost. Reinstalling
    // it would rewind the state machine to an older image.
    auto again = raft.HandleInstallSnapshot(req);
    if (!again.success) {
        std::fprintf(stderr, "  replayed InstallSnapshot was not acked\n");
    }
    assert(again.success);
    if (sm.applied_snapshots_ != 1) {
        std::fprintf(stderr, "  replayed InstallSnapshot re-applied the state "
                             "machine (%d times)\n",
                     sm.applied_snapshots_);
    }
    assert(sm.applied_snapshots_ == 1);

    // The installed baseline must also survive a restart.
    raft.Stop();

    CountingSnapshotSM sm2;
    NullRPC rpc2;
    Raft raft2(MakeSingleNodeOptions(), &sm2, &rpc2);
    assert(raft2.Initialize(dir).ok());
    if (raft2.GetLastSnapshotIndex() != 40) {
        std::fprintf(stderr, "  installed baseline lost across restart (%llu)\n",
                     (unsigned long long)raft2.GetLastSnapshotIndex());
    }
    assert(raft2.GetLastSnapshotIndex() == 40);
    assert(raft2.GetSnapshotData() == "STATE-IMAGE");
    raft2.Stop();

    lightkv_remove_tree(dir);
}

// Followers must never compact on their own: they inherit the leader's
// baseline through InstallSnapshot, and dropping entries their leader may still
// need would strand a third node.
void TestFollowerDoesNotCompactLog() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_follower_nocompact";
    lightkv_remove_tree(dir);

    WorkingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    SeedAppliedEntries(&raft, 20);
    // SeedAppliedEntries drives the node through HandleAppendEntries, which
    // puts it in the follower role.
    assert(raft.GetRole() != RaftRole::kLeader);

    const uint64_t before = raft.GetStats().log_count;
    raft.CheckAndCompactLog();  // 20 entries is far below the threshold
    const uint64_t after = raft.GetStats().log_count;
    if (after != before) {
        std::fprintf(stderr, "  a follower compacted its log (%llu -> %llu)\n",
                     (unsigned long long)before, (unsigned long long)after);
    }
    assert(after == before);
    assert(raft.GetLastSnapshotIndex() == 0);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// Node 1 is the only node allowed to campaign, so leadership is deterministic:
// it self-elects within a few hundred ms and the other two never time out.
// If any node could campaign, a leadership change mid-test would let a leader
// that had not compacted ship raw log entries instead of a snapshot, making the
// test flaky without testing anything wrong.
static RaftOptions MakeClusterOptions(uint64_t node_id) {
    RaftOptions opts;
    opts.node_id = node_id;
    opts.peers.push_back(RaftPeer{1, "127.0.0.1", 16379, true});
    opts.peers.push_back(RaftPeer{2, "127.0.0.1", 16380, true});
    opts.peers.push_back(RaftPeer{3, "127.0.0.1", 16381, true});
    opts.enable_raft = true;
    if (node_id == 1) {
        opts.election_timeout_min_ms = 100;
        opts.election_timeout_max_ms = 100;
    } else {
        opts.election_timeout_min_ms = 60000;
        opts.election_timeout_max_ms = 120000;
    }
    opts.heartbeat_interval_ms = 20;
    return opts;
}

// End-to-end: a follower that was offline while the leader compacted its log can
// only be caught up by shipping the snapshot. Walking next_index_ back to index 1
// would not help, because those entries exist only inside the snapshot.
//
// Before this was wired, ReplicateLog() had a dead branch where the "follower is
// too far behind" test was written as "follower needs entries", and
// SendSnapshotToFollower() was declared but never defined.
void TestLeaderShipsSnapshotToLaggingFollower() {
    const std::string base = std::string(LIGHTKV_TEST_TMP) + "/lightkv_raft_lag";
    lightkv_remove_tree(base);

    RoutingRPC rpc;
    std::map<uint64_t, std::shared_ptr<CountingSnapshotSM>> sms;
    std::map<uint64_t, std::shared_ptr<Raft>> nodes;

    auto spawn = [&](uint64_t id) -> std::shared_ptr<Raft> {
        auto sm = std::make_shared<CountingSnapshotSM>();
        sms[id] = sm;
        auto node = std::make_shared<Raft>(MakeClusterOptions(id), sm.get(), &rpc);
        const std::string dir = base + "/node" + std::to_string(id);
        Status s = node->Initialize(dir);
        assert(s.ok());
        nodes[id] = node;
        rpc.Add(id, node);
        node->Start();
        return node;
    };

    spawn(1);
    spawn(2);
    auto n3 = spawn(3);

    std::shared_ptr<Raft> leader;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        for (auto& kv : nodes) {
            if (kv.second->GetRole() == RaftRole::kLeader) {
                leader = kv.second;
                break;
            }
        }
        if (leader) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!leader) {
        std::fprintf(stderr, "  no leader elected in the test cluster\n");
    }
    assert(leader != nullptr);

    // Give the cluster some committed history.
    for (int i = 0; i < 5; ++i) {
        if (leader->ProposeSync(std::string("cmd") + std::to_string(i), 5000)) {
            continue;
        }
        std::fprintf(stderr, "  ProposeSync failed for entry %d\n", i);
        assert(false);
    }
    const uint64_t committed = leader->GetStats().last_applied;
    assert(committed > 0);

    // The leader snapshots and compacts away everything it just wrote.
    if (!leader->TriggerSnapshot()) {
        std::fprintf(stderr, "  leader refused to snapshot its own state machine\n");
    }
    assert(leader->TriggerSnapshot());
    const uint64_t snap_index = leader->GetLastSnapshotIndex();
    assert(snap_index == committed);

    // Node 2 goes offline and loses its log entirely: it is now further behind
    // than the leader's retained log.
    rpc.Remove(2);
    nodes[2]->Stop();
    nodes.erase(2);
    rpc.Remove(2);

    const std::string node2_dir = base + "/node2";
    lightkv_remove_tree(node2_dir);
    auto sm2 = std::make_shared<CountingSnapshotSM>();
    sms[2] = sm2;
    auto reborn = std::make_shared<Raft>(MakeClusterOptions(2), sm2.get(), &rpc);
    assert(reborn->Initialize(node2_dir).ok());
    nodes[2] = reborn;
    rpc.Add(2, reborn);
    reborn->Start();

    rpc.ResetCounters();
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        if (reborn->GetStats().last_applied >= committed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    const uint64_t caught_up = reborn->GetStats().last_applied;
    std::fprintf(stderr,
                 "  [raft] leader baseline=%llu, reborn follower applied=%llu "
                 "(target %llu), snapshots sent=%d, state machine installs=%d\n",
                 (unsigned long long)snap_index, (unsigned long long)caught_up,
                 (unsigned long long)committed, rpc.snapshots_sent(),
                 sm2->applied_snapshots_);

    if (rpc.snapshots_sent() == 0) {
        std::fprintf(stderr, "  leader never shipped a snapshot to the lagging follower\n");
    }
    assert(rpc.snapshots_sent() > 0);
    if (sm2->applied_snapshots_ == 0) {
        std::fprintf(stderr, "  follower never installed the snapshot into its state machine\n");
    }
    assert(sm2->applied_snapshots_ > 0);
    if (caught_up < committed) {
        std::fprintf(stderr, "  follower did not catch up (%llu < %llu)\n",
                     (unsigned long long)caught_up, (unsigned long long)committed);
    }
    assert(caught_up >= committed);

    for (auto& kv : nodes) {
        if (kv.second) kv.second->Stop();
    }
    lightkv_remove_tree(base);
}

// A snapshot the state machine cannot install must be reported as a failure, and
// must leave the node's log and indices completely untouched.
//
// ApplySnapshot() used to return void, so HandleInstallSnapshot() truncated the
// log and reported success unconditionally. The leader then advanced
// next_index_/match_index_ for this node and counted it toward the majority --
// while it had applied nothing. That is silent divergence, not a lost error
// message.
void TestFailedInstallIsReportedAndNonDestructive() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_raft_install_fail";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeSingleNodeOptions(), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    SeedAppliedEntries(&raft, 20);
    const uint64_t log_before = raft.GetStats().log_count;
    const uint64_t commit_before = raft.GetStats().commit_index;
    const uint64_t applied_before = raft.GetStats().last_applied;

    InstallSnapshotRequest req{};
    req.term = raft.GetCurrentTerm();
    req.leader_id = 1;
    req.last_included_index = 30;
    req.last_included_term = 3;
    req.data = "STATE-IMAGE";

    sm.fail_install_ = true;
    auto resp = raft.HandleInstallSnapshot(req);
    if (resp.success) {
        std::fprintf(stderr,
                     "  InstallSnapshot reported success even though the state "
                     "machine rejected it\n");
    }
    assert(!resp.success);

    const auto after = raft.GetStats();
    std::fprintf(stderr,
                 "  [raft] failed install: log %llu->%llu commit %llu->%llu "
                 "applied %llu->%llu\n",
                 (unsigned long long)log_before, (unsigned long long)after.log_count,
                 (unsigned long long)commit_before, (unsigned long long)after.commit_index,
                 (unsigned long long)applied_before, (unsigned long long)after.last_applied);
    if (after.log_count != log_before || after.commit_index != commit_before ||
        after.last_applied != applied_before) {
        std::fprintf(stderr, "  a rejected InstallSnapshot mutated the node\n");
    }
    assert(after.log_count == log_before);
    assert(after.commit_index == commit_before);
    assert(after.last_applied == applied_before);
    assert(raft.GetLastSnapshotIndex() == 0);
    assert(raft.GetSnapshotData().empty());

    // Retrying once the state machine recovers must succeed.
    sm.fail_install_ = false;
    auto retry = raft.HandleInstallSnapshot(req);
    if (!retry.success) {
        std::fprintf(stderr, "  InstallSnapshot still failing after recovery\n");
    }
    assert(retry.success);
    assert(raft.GetLastSnapshotIndex() == 30);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// ─── Chunked snapshot transfer ───────────────────────────────────────
//
// Regression context: InstallSnapshot carried the whole payload in one message,
// with a uint32_t length and a std::string sized to match. Anything past 4GB was
// unsendable, and RecvExact() had no timeout at all, so a peer that stopped
// responding wedged the leader's replication thread permanently.

static RaftOptions MakeChunkOptions(uint64_t node_id, size_t chunk, size_t budget) {
    RaftOptions opts;
    opts.node_id = node_id;
    opts.peers.push_back(RaftPeer{node_id, "127.0.0.1", 16379, true});
    opts.peers.push_back(RaftPeer{99, "127.0.0.1", 16399, true});
    opts.enable_raft = true;
    opts.election_timeout_min_ms = 60000;
    opts.election_timeout_max_ms = 120000;
    opts.heartbeat_interval_ms = 50;
    opts.snapshot_chunk_size = chunk;
    opts.snapshot_send_budget = budget;
    return opts;
}

// A chunked transfer must reassemble to exactly the original payload and apply
// it once, no matter how it was sliced.
void TestChunkedSnapshotRoundTrip() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_chunk_rt";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeChunkOptions(1, 64, 4096), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    // 1000 bytes over 64-byte chunks => 16 chunks.
    std::string payload;
    for (int i = 0; i < 1000; ++i) payload.push_back(static_cast<char>('a' + (i % 26)));
    const uint64_t total = payload.size();

    uint64_t offset = 0;
    int chunks = 0;
    InstallSnapshotChunkResponse last;
    while (offset < total) {
        const size_t take = std::min<size_t>(64, payload.size() - offset);
        InstallSnapshotChunkRequest req;
        req.term = raft.GetCurrentTerm();
        req.leader_id = 1;
        req.last_included_index = 10;
        req.last_included_term = 2;
        req.chunk_offset = offset;
        req.total_size = total;
        req.is_last = (offset + take >= total);
        req.data = payload.substr(offset, take);

        last = raft.HandleInstallSnapshotChunk(req);
        if (last.rejected) {
            std::fprintf(stderr, "  chunk %d at offset %llu rejected\n", chunks,
                         (unsigned long long)offset);
        }
        assert(!last.rejected);
        ++chunks;
        offset = last.accepted_len;
    }

    std::fprintf(stderr, "  [chunk] %lld bytes in %d chunks, complete=%d\n",
                 (unsigned long long)total, chunks, (int)last.complete);
    assert(last.complete);
    assert(sm.applied_snapshots_ == 1);
    assert(sm.last_ == payload);
    assert(raft.GetLastSnapshotIndex() == 10);
    assert(raft.GetLastSnapshotTerm() == 2);
    assert(raft.GetSnapshotData() == payload);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// A resumed transfer must not duplicate bytes: a chunk the follower already has
// is re-sent after a torn write.
void TestChunkedSnapshotResumesWithoutDuplicating() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_chunk_resume";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeChunkOptions(1, 64, 4096), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    std::string payload(300, 'z');

    auto send = [&](uint64_t off, size_t take, bool is_last) {
        InstallSnapshotChunkRequest req;
        req.term = raft.GetCurrentTerm();
        req.leader_id = 1;
        req.last_included_index = 7;
        req.last_included_term = 3;
        req.chunk_offset = off;
        req.total_size = payload.size();
        req.is_last = is_last;
        req.data = payload.substr(off, take);
        return raft.HandleInstallSnapshotChunk(req);
    };

    // First two chunks.
    auto r1 = send(0, 64, false);
    auto r2 = send(64, 64, false);
    assert(!r1.rejected && r1.accepted_len == 64);
    assert(!r2.rejected && r2.accepted_len == 128);

    // Re-send chunk 1 (already held). The follower must replace, not append.
    auto again = send(64, 64, false);
    if (again.accepted_len != 128) {
        std::fprintf(stderr, "  resume produced %llu bytes, expected 128\n",
                     (unsigned long long)again.accepted_len);
    }
    assert(again.accepted_len == 128);

    // Finish.
    auto last = send(128, 64, false);
    assert(!last.rejected && last.accepted_len == 192);
    auto fin = send(192, 108, true);
    assert(fin.complete);
    assert(sm.applied_snapshots_ == 1);
    assert(sm.last_ == payload);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// Every rejection path: oversize, gap, and inconsistent arithmetic.
void TestChunkedSnapshotRejectsBadInput() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_chunk_reject";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeChunkOptions(1, 64, 4096), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    auto base = [&](uint64_t total) {
        InstallSnapshotChunkRequest req;
        req.term = raft.GetCurrentTerm();
        req.leader_id = 1;
        req.last_included_index = 5;
        req.last_included_term = 1;
        req.chunk_offset = 0;
        req.total_size = total;
        req.is_last = false;
        req.data = std::string(64, 'x');
        return req;
    };

    // Larger than max_snapshot_size (default 256MB) -- a leader must not be able
    // to dictate how much memory a follower allocates.
    Raft small(MakeChunkOptions(1, 64, 4096), &sm, &rpc);
    RaftOptions tight = MakeChunkOptions(1, 64, 4096);
    tight.max_snapshot_size = 1024;
    {
        Raft guarded(tight, &sm, &rpc);
        assert(guarded.Initialize(LIGHTKV_TEST_TMP "/lightkv_chunk_tight").ok());
        InstallSnapshotChunkRequest req = base(4096);
        auto resp = guarded.HandleInstallSnapshotChunk(req);
        if (!resp.rejected) {
            std::fprintf(stderr, "  oversized snapshot was accepted\n");
        }
        assert(resp.rejected);
        assert(resp.accepted_len == 0);
    }
    lightkv_remove_tree(LIGHTKV_TEST_TMP "/lightkv_chunk_tight");

    // Zero total size.
    auto zero = raft.HandleInstallSnapshotChunk(base(0));
    assert(zero.rejected);

    // offset + len beyond total.
    auto over = base(100);
    over.chunk_offset = 80;
    over.data = std::string(64, 'x');
    auto over_resp = raft.HandleInstallSnapshotChunk(over);
    if (!over_resp.rejected) {
        std::fprintf(stderr, "  chunk running past the declared total was accepted\n");
    }
    assert(over_resp.rejected);

    // A gap: skip ahead without sending the bytes in between. Silently accepting
    // would splice two disjoint ranges into one corrupt image.
    auto first = base(400);
    auto gap = base(400);
    gap.chunk_offset = 200;
    auto gap_resp = raft.HandleInstallSnapshotChunk(gap);
    if (!gap_resp.rejected) {
        std::fprintf(stderr, "  chunk with a gap was accepted\n");
    }
    assert(gap_resp.rejected);

    // Nothing should have been installed by any of the above.
    assert(sm.applied_snapshots_ == 0);
    assert(raft.GetLastSnapshotIndex() == 0);

    raft.Stop();
    lightkv_remove_tree(dir);
}

// A newer snapshot supersedes a half-finished one; splicing them would produce
// an image that belongs to neither.
void TestChunkedSnapshotIdentityChangeDiscardsPartial() {
    std::string dir = LIGHTKV_TEST_TMP "/lightkv_chunk_identity";
    lightkv_remove_tree(dir);

    CountingSnapshotSM sm;
    NullRPC rpc;
    Raft raft(MakeChunkOptions(1, 64, 4096), &sm, &rpc);
    assert(raft.Initialize(dir).ok());

    // total_size must be the size of the WHOLE image, not of this slice.
    auto chunk = [&](uint64_t index, uint64_t off, uint64_t total,
                     const std::string& bytes) {
        InstallSnapshotChunkRequest req;
        req.term = raft.GetCurrentTerm();
        req.leader_id = 1;
        req.last_included_index = index;
        req.last_included_term = 1;
        req.chunk_offset = off;
        req.total_size = total;
        req.is_last = false;
        req.data = bytes;
        return raft.HandleInstallSnapshotChunk(req);
    };

    std::string old_image(200, 'o');
    std::string new_image(200, 'n');

    auto a = chunk(10, 0, old_image.size(), old_image.substr(0, 100));
    assert(!a.rejected && a.accepted_len == 100);
    assert(!a.complete);

    // A different snapshot arrives mid-transfer: the partial must be dropped.
    auto b = chunk(11, 0, new_image.size(), new_image.substr(0, 100));
    if (!b.rejected || b.accepted_len != 100) {
        std::fprintf(stderr,
                     "  identity change did not reset staging "
                     "(accepted=%llu rejected=%d)\n",
                     (unsigned long long)b.accepted_len, (int)b.rejected);
    }
    assert(!b.rejected);
    assert(b.accepted_len == 100);
    assert(!b.complete);

    // Completing the NEW snapshot must yield exactly the new image -- if the old
    // bytes had survived, the result would be 200 'o'/'n' or fail outright.
    InstallSnapshotChunkRequest fin;
    fin.term = raft.GetCurrentTerm();
    fin.leader_id = 1;
    fin.last_included_index = 11;
    fin.last_included_term = 1;
    fin.chunk_offset = 100;
    fin.total_size = new_image.size();
    fin.is_last = true;
    fin.data = new_image.substr(100);
    auto last = raft.HandleInstallSnapshotChunk(fin);
    assert(last.complete);
    assert(sm.last_ == new_image);
    assert(raft.GetLastSnapshotIndex() == 11);

    raft.Stop();
    lightkv_remove_tree(dir);
}

int main() {
    TestSnapshotRefusedWhenStateMachineCannotSnapshot();
    TestSnapshotCompactsWhenStateMachineWorks();
    TestEmptyInstallSnapshotRejected();
    TestSnapshotSurvivesRestart();
    TestInstallSnapshotRecordsBaselineAndIsIdempotent();
    TestFollowerDoesNotCompactLog();
    TestFailedInstallIsReportedAndNonDestructive();
    TestLeaderShipsSnapshotToLaggingFollower();
    TestChunkedSnapshotRoundTrip();
    TestChunkedSnapshotResumesWithoutDuplicating();
    TestChunkedSnapshotRejectsBadInput();
    TestChunkedSnapshotIdentityChangeDiscardsPartial();

    std::cout << "All Raft tests passed!" << std::endl;
    return 0;
}
