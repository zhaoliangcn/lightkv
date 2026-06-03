#include "lightkv/table_builder.h"
#include "lightkv/sstable.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>

namespace lightkv {

void BlockHandle::EncodeTo(std::string* dst) const {
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
}

BlockHandle BlockHandle::DecodeFrom(const Slice& input) {
    BlockHandle handle;
    const char* p = input.data();
    const char* limit = input.data() + input.size();
    p = GetVarint64(p, limit, &handle.offset);
    p = GetVarint64(p, limit, &handle.size);
    return handle;
}

FileWriter::FileWriter(const std::string& filename)
    : filename_(filename), fd_(-1), offset_(0) {}

FileWriter::~FileWriter() { Close(); }

bool FileWriter::Open() {
    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    return fd_ >= 0;
}

void FileWriter::Append(const Slice& data) {
    ::pwrite(fd_, data.data(), data.size(), static_cast<off_t>(offset_));
    offset_ += data.size();
}

void FileWriter::Flush() {
    // fsync done in Sync()
}

void FileWriter::Sync() {
    ::fsync(fd_);
}

void FileWriter::Close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

TableBuilder::TableBuilder(const Options& options, const std::string& filename)
    : options_(options),
      writer_(std::make_unique<FileWriter>(filename)),
      data_block_(options.block_size),
      index_block_(options.restart_interval),
      bloom_filter_(options.bloom_bits_per_key),
      num_entries_(0),
      finished_(false) {
    writer_->Open();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
    if (finished_) return;

    bloom_filter_.Add(key);
    data_block_.Add(key, value);
    ++num_entries_;
    last_key_.assign(key.data(), key.size());

    if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
        FlushDataBlock();
    }
}

void TableBuilder::FlushDataBlock() {
    if (data_block_.empty()) return;

    Slice block_data = data_block_.Finish();
    DataBlockInfo info;
    info.handle.offset = writer_->Offset();
    info.handle.size = block_data.size();
    info.last_key = last_key_;

    writer_->Append(block_data);

    std::string encoded;
    info.handle.EncodeTo(&encoded);
    index_block_.Add(Slice(info.last_key), Slice(encoded));
    data_blocks_.push_back(std::move(info));

    data_block_.Reset();
}

void TableBuilder::Finish() {
    if (finished_) return;
    FlushDataBlock();

    // Write bloom filter
    std::string bloom_data(bloom_filter_.data(), bloom_filter_.size());
    BlockHandle bloom_handle;
    bloom_handle.offset = writer_->Offset();
    bloom_handle.size = bloom_data.size();
    writer_->Append(Slice(bloom_data));

    // Write index block
    Slice index_data = index_block_.Finish();
    BlockHandle index_handle;
    index_handle.offset = writer_->Offset();
    index_handle.size = index_data.size();
    writer_->Append(index_data);

    // Write footer
    TableFooter footer;
    footer.index_block_handle_offset = index_handle.offset;
    footer.meta_index_block_handle_offset = bloom_handle.offset;

    std::string footer_data;
    footer.EncodeTo(&footer_data);
    writer_->Append(Slice(footer_data));

    writer_->Sync();
    writer_->Close();
    finished_ = true;
}

void TableBuilder::Abandon() {
    writer_->Close();
    ::unlink(writer_->Offset() > 0 ? "output" : "");
}

} // namespace lightkv