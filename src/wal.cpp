#include "lightkv/wal.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>

namespace lightkv {

static constexpr size_t kInitialFileSize = 64 * 1024 * 1024;
static constexpr size_t kBlockSize = 4096;

WALWriter::WALWriter(const std::string& filename)
    : filename_(filename), fd_(-1), mmap_base_(nullptr),
      file_size_(kInitialFileSize), write_pos_(0) {}

WALWriter::~WALWriter() { Close(); }

Status WALWriter::Open() {
    fd_ = ::open(filename_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return Status::IOError("cannot open WAL file");

    if (::ftruncate(fd_, static_cast<off_t>(file_size_)) < 0) {
        ::close(fd_);
        fd_ = -1;
        return Status::IOError("cannot truncate WAL file");
    }

    mmap_base_ = ::mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mmap_base_ == MAP_FAILED) {
        ::close(fd_);
        fd_ = -1;
        mmap_base_ = nullptr;
        return Status::IOError("cannot mmap WAL file");
    }

    return Status::OK();
}

Status WALWriter::GrowFile() {
    ::munmap(mmap_base_, file_size_);
    size_t new_size = file_size_ * 2;
    if (::ftruncate(fd_, static_cast<off_t>(new_size)) < 0) {
        mmap_base_ = nullptr;
        return Status::IOError("cannot grow WAL file");
    }
    mmap_base_ = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mmap_base_ == MAP_FAILED) {
        mmap_base_ = nullptr;
        return Status::IOError("cannot remap WAL file");
    }
    file_size_ = new_size;
    return Status::OK();
}

Status WALWriter::Append(uint64_t seq, WALRecord::Type type, const Slice& key, const Slice& value) {
    uint32_t key_size = static_cast<uint32_t>(key.size());
    uint32_t val_size = static_cast<uint32_t>(value.size());
    int key_varint_len = VarintLength(key_size);
    int val_varint_len = VarintLength(val_size);

    uint32_t record_len = 1 + key_varint_len + key_size + val_varint_len + val_size + 8;
    uint32_t total = 8 + record_len;

    if (write_pos_ + total + kBlockSize >= file_size_) {
        auto s = GrowFile();
        if (!s.ok()) return s;
    }

    char* buf = static_cast<char*>(mmap_base_) + write_pos_;

    char type_byte = static_cast<char>(type);
    uint32_t crc = Crc32cExtend(0, &type_byte, 1);
    crc = Crc32cExtend(crc, reinterpret_cast<const char*>(&key_size), sizeof(uint32_t));
    crc = Crc32cExtend(crc, key.data(), key_size);
    crc = Crc32cExtend(crc, reinterpret_cast<const char*>(&val_size), sizeof(uint32_t));
    crc = Crc32cExtend(crc, value.data(), val_size);
    crc = Crc32cExtend(crc, reinterpret_cast<const char*>(&seq), sizeof(seq));

    EncodeFixed32(buf, crc);
    EncodeFixed32(buf + 4, record_len);
    char* p = buf + 8;
    *p++ = type_byte;
    p = EncodeVarint32(p, key_size);
    memcpy(p, key.data(), key_size);
    p += key_size;
    p = EncodeVarint32(p, val_size);
    memcpy(p, value.data(), val_size);
    p += val_size;
    EncodeFixed64(p, seq);

    write_pos_ += total;
    return Status::OK();
}

Status WALWriter::Sync() {
    if (mmap_base_) {
        ::msync(mmap_base_, write_pos_, MS_SYNC);
        ::fsync(fd_);
    }
    return Status::OK();
}

void WALWriter::Close() {
    if (mmap_base_ && mmap_base_ != MAP_FAILED) {
        Sync();
        ::munmap(mmap_base_, file_size_);
        mmap_base_ = nullptr;
    }
    if (fd_ >= 0) {
        if (::ftruncate(fd_, static_cast<off_t>(write_pos_)) < 0) {
            // best effort
        }
        ::close(fd_);
        fd_ = -1;
    }
}

WALReader::WALReader(const std::string& filename)
    : filename_(filename), fd_(-1), mmap_base_(nullptr),
      file_size_(0), read_pos_(0) {}

WALReader::~WALReader() { Close(); }

Status WALReader::Open() {
    fd_ = ::open(filename_.c_str(), O_RDONLY);
    if (fd_ < 0) return Status::IOError("cannot open WAL file for reading");

    struct stat st;
    if (::fstat(fd_, &st) < 0) {
        ::close(fd_);
        fd_ = -1;
        return Status::IOError("cannot stat WAL file");
    }
    file_size_ = static_cast<size_t>(st.st_size);
    if (file_size_ > 0) {
        mmap_base_ = ::mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (mmap_base_ == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            return Status::IOError("cannot mmap WAL for reading");
        }
    }

    return Status::OK();
}

bool WALReader::ReadRecord(WALRecord* record) {
    if (read_pos_ + 8 > file_size_) return false;

    const char* base = static_cast<const char*>(mmap_base_);
    uint32_t crc = DecodeFixed32(base + read_pos_);
    uint32_t len = DecodeFixed32(base + read_pos_ + 4);

    if (read_pos_ + 8 + len > file_size_) return false;

    const char* record_start = base + read_pos_ + 8;
    uint32_t actual_crc = Crc32c(record_start, len);
    if (crc != actual_crc) return false;

    read_pos_ += 8;

    record->type = static_cast<WALRecord::Type>(record_start[0]);
    const char* p = record_start + 1;

    uint32_t key_len;
    p = GetVarint32(p, record_start + len, &key_len);
    record->key.assign(p, key_len);
    p += key_len;

    uint32_t value_len;
    p = GetVarint32(p, record_start + len, &value_len);
    record->value.assign(p, value_len);
    p += value_len;

    record->seq = DecodeFixed64(p);

    read_pos_ += len;
    return true;
}

Status WALReader::Close() {
    if (mmap_base_ && mmap_base_ != MAP_FAILED) {
        ::munmap(mmap_base_, file_size_);
        mmap_base_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

} // namespace lightkv