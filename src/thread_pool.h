// Worker pool used when ServerOptions::worker_threads > 0.
//
// Split out of server.cpp so the command handlers can live in their own
// translation units; see server_internal.h for the shared declarations.
#ifndef LIGHTKV_THREAD_POOL_H
#define LIGHTKV_THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace lightkv {
namespace internal {

struct WorkerTask {
    int conn_fd;
    std::vector<std::string> args;
    std::function<std::string(const std::vector<std::string>&)> handler;
};

class ThreadPool {
public:
    ThreadPool(int num_threads, std::function<void(int, std::string)> on_complete);
    ~ThreadPool();

    // Stop accepting work and join the workers. Idempotent.
    //
    // Workers execute command handlers that touch the DB, so this must happen
    // before the caller is allowed to destroy the DB -- not only in the
    // destructor, which typically runs much later.
    void Stop();

    int wakeup_read_fd() const { return wakeup_read_fd_; }
    void submit(WorkerTask task);
    void wakeup_event_loop();
    void clear_wakeup();

private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::queue<WorkerTask> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::function<void(int, std::string)> on_complete_;
    bool stop_;
    int wakeup_fd_;       // write end (pipe or eventfd)
    int wakeup_read_fd_;  // read end (for event loop to monitor)
};

}  // namespace internal
}  // namespace lightkv

#endif  // LIGHTKV_THREAD_POOL_H