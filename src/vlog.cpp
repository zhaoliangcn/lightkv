#include "lightkv/vlog.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstring>
#include <cstdio>
#include <sstream>
#include <filesystem>

namespace lightkv {

static constexpr size_t kInitialVLogSize = 64 * 1024 * 1024;  // 64MB

// ─── VLog ───

VLog::VLog(const std::string& db_path, uint64_t file_id, size_t initial_size)
    : file_id_(file_id), fd_(-1), mmap_base_(nullptr),
      file_size_(initial_size > 0 ? initial_size : kInitialVLogSize),
      write_pos_(0) {
    std::ostringstream ss;
    ss << db_path << "/vlog-" << file_id << ".log";
    path_ = ss.str();
}

VLog::~VLog() { Close(); }

Status VLog::Open() {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) return Status::IOError("cannot open vlog file");

    // 已存在的 vlog：stat 取实际大小并更新 file_size_ / write_pos_
    struct stat st;
    if (::fstat(fd_, &st) == 0 && st.st_size > 0) {
        if (static_cast<size_t>(st.st_size) > file_size_) {
            file_size_ = static_cast<size_t>(st.st_size);
        }
        write_pos_ = static_cast<uint64_t>(st.st_size);
    }

    if (::ftruncate(fd_, static_cast<off_t>(file_size_)) < 0) {
        ::close(fd_);
        fd_ = -1;
        return Status::IOError("cannot truncate vlog file");
    }

    mmap_base_ = ::mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mmap_base_ == MAP_FAILED) {
        ::close(fd_);
        fd_ = -1;
        mmap_base_ = nullptr;
        return Status::IOError("cannot mmap vlog file");
    }
    return Status::OK();
}

Status VLog::GrowFile() {
    if (mmap_base_ && mmap_base_ != MAP_FAILED) {
        ::munmap(mmap_base_, file_size_);
        mmap_base_ = nullptr;
    }
    size_t new_size = file_size_ * 2;
    if (::ftruncate(fd_, static_cast<off_t>(new_size)) < 0) {
        return Status::IOError("cannot grow vlog file");
    }
    mmap_base_ = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mmap_base_ == MAP_FAILED) {
        mmap_base_ = nullptr;
        return Status::IOError("cannot remap vlog file");
    }
    file_size_ = new_size;
    return Status::OK();
}

Status VLog::Append(const Slice& value, uint64_t* out_offset, uint64_t* out_length) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t need = static_cast<size_t>(write_pos_) + value.size() + 4096;  // 4KB 余量
    if (need > file_size_) {
        auto s = GrowFile();
        if (!s.ok()) return s;
    }

    char* dst = static_cast<char*>(mmap_base_) + write_pos_;
    ::memcpy(dst, value.data(), value.size());
    *out_offset = write_pos_;
    *out_length = static_cast<uint64_t>(value.size());
    write_pos_ += value.size();
    return Status::OK();
}

Status VLog::Read(uint64_t offset, uint64_t length, std::string* out) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (offset + length > write_pos_) {
        return Status::IOError("vlog read out of range");
    }
    const char* src = static_cast<const char*>(mmap_base_) + offset;
    out->assign(src, length);
    return Status::OK();
}

Status VLog::Sync() {
    std::lock_guard<std::mutex> lock(mu_);
    if (mmap_base_) {
        ::msync(mmap_base_, static_cast<size_t>(write_pos_), MS_SYNC);
        ::fsync(fd_);
    }
    return Status::OK();
}

void VLog::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (mmap_base_ && mmap_base_ != MAP_FAILED) {
        // Sync only the written portion (best effort, ignore return)
        (void)::msync(mmap_base_, static_cast<size_t>(write_pos_), MS_SYNC);
        (void)::fsync(fd_);
        ::munmap(mmap_base_, file_size_);
        mmap_base_ = nullptr;
    }
    if (fd_ >= 0) {
        // Truncate to actual written size (best effort, ignore return)
        if (write_pos_ > 0) (void)::ftruncate(fd_, static_cast<off_t>(write_pos_));
        ::close(fd_);
        fd_ = -1;
    }
    write_pos_ = 0;  // guard against re-entry
}

// ─── VLogManager ───

VLogManager::VLogManager(const std::string& db_path, size_t file_size_limit)
    : db_path_(db_path), file_size_limit_(file_size_limit), next_file_id_(1) {}

VLogManager::~VLogManager() { Close(); }

Status VLogManager::Initialize() {
    std::lock_guard<std::mutex> lock(mu_);
    // 扫描 db_path 下所有 vlog-{id}.log，记录 next_file_id_
    uint64_t max_id = 0;
    try {
        for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
            std::string name = entry.path().filename().string();
            if (name.rfind("vlog-", 0) == 0) {
                size_t dot = name.find('.');
                if (dot != std::string::npos) {
                    std::string id_str = name.substr(5, dot - 5);
                    try {
                        uint64_t id = std::stoull(id_str);
                        if (id > max_id) max_id = id;
                    } catch (...) {}
                }
            }
        }
    } catch (...) {
        // 目录不存在或读取失败 → next_file_id_ 保持 1
    }
    next_file_id_ = max_id + 1;

    // 打开/创建活动 vlog
    uint64_t active_id = next_file_id_++;
    active_ = std::make_unique<VLog>(db_path_, active_id);
    return active_->Open();
}

Status VLogManager::Append(const Slice& value,
                           uint64_t* out_file_id,
                           uint64_t* out_offset,
                           uint64_t* out_length) {
    std::lock_guard<std::mutex> lock(mu_);
    // 活动 vlog 空间不足（写入后超过 limit）→ 滚动到新文件
    if (active_ && active_->Size() + value.size() > file_size_limit_) {
        active_->Sync();
        active_->Close();
        uint64_t new_id = next_file_id_++;
        active_ = std::make_unique<VLog>(db_path_, new_id);
        auto s = active_->Open();
        if (!s.ok()) return s;
    }
    if (!active_) {
        return Status::IOError("vlog manager not initialized");
    }
    *out_file_id = active_->file_id();
    return active_->Append(value, out_offset, out_length);
}

Status VLogManager::Read(uint64_t file_id, uint64_t offset, uint64_t length, std::string* out) {
    std::lock_guard<std::mutex> lock(mu_);
    // 活动 vlog 命中
    if (active_ && active_->file_id() == file_id) {
        return active_->Read(offset, length, out);
    }
    // 读取缓存命中
    auto it = read_cache_.find(file_id);
    if (it != read_cache_.end()) {
        return it->second->Read(offset, length, out);
    }
    // 打开对应 vlog 文件并缓存
    auto vl = std::make_unique<VLog>(db_path_, file_id);
    auto s = vl->Open();
    if (!s.ok()) return s;
    VLog* raw = vl.get();
    read_cache_[file_id] = std::move(vl);
    return raw->Read(offset, length, out);
}

Status VLogManager::TriggerGC() {
    // Phase 1 仅占位，Phase 2 实装（详见设计草案 7.5）
    return Status::OK();
}

void VLogManager::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (active_) {
        active_->Close();
        active_.reset();
    }
    read_cache_.clear();
}

} // namespace lightkv