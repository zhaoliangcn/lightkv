#include "thread_pool.h"

#include "lightkv/platform.h"

#ifdef __APPLE__
#include <unistd.h>
#elif !defined(_WIN32)
#include <unistd.h>
#endif

namespace lightkv {
namespace internal {

ThreadPool::ThreadPool(int num_threads,
                       std::function<void(int, std::string)> on_complete)
    : on_complete_(std::move(on_complete)), stop_(false), wakeup_fd_(-1) {
    int pipefd[2];
    if (platform_pipe(pipefd) == 0) {
        wakeup_fd_ = pipefd[1];  // write end
        wakeup_read_fd_ = pipefd[0];  // read end
        // Make both ends non-blocking
        socket_set_nonblocking(wakeup_fd_);
        socket_set_nonblocking(wakeup_read_fd_);
    }
    for (int i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this]() { worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    Stop();
    if (wakeup_fd_ >= 0) socket_close(wakeup_fd_);
#ifdef __APPLE__
    if (wakeup_read_fd_ >= 0) socket_close(wakeup_read_fd_);
#endif
}

void ThreadPool::Stop() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_ = true;
        cv_.notify_all();
    }
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
    workers_.clear();
}

void ThreadPool::submit(WorkerTask task) {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push(std::move(task));
    cv_.notify_one();
}

void ThreadPool::wakeup_event_loop() {
    if (wakeup_fd_ >= 0) {
        char buf = 1;
        ::write(wakeup_fd_, &buf, 1);
    }
}

void ThreadPool::clear_wakeup() {
    if (wakeup_read_fd_ >= 0) {
        char buf[64];
        // Drain the wakeup pipe
        while (::read(wakeup_read_fd_, buf, sizeof(buf)) > 0) {}
    }
}

void ThreadPool::worker_loop() {
    while (true) {
        WorkerTask task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) return;
            task = std::move(queue_.front());
            queue_.pop();
        }

        // Execute command
        std::string resp = task.handler(task.args);

        // Notify event loop
        on_complete_(task.conn_fd, std::move(resp));

        // Wake up the event loop so it can drain responses
        wakeup_event_loop();
    }
}

}  // namespace internal
}  // namespace lightkv