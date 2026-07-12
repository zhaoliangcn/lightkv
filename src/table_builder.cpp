#include "lightkv/table_builder.h"
#include "lightkv/sstable.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#ifdef HAVE_LZ4
#include <lz4.h>
#endif

namespace lightkv {

FileWriter::FileWriter(const std::string& filename)
    : filename_(filename), fd_(-1), offset_(0) {}

FileWriter::~FileWriter() { Close(); }

bool FileWriter::Open() {
    fd_ = ::open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    return fd_ >= 0;
}

void FileWriter::Append(const Slice& data) {
    size_t written = 0;
    while (written < data.size()) {
        ssize_t n = ::pwrite(fd_, data.data() + written, data.size() - written,
                             static_cast<off_t>(offset_ + written));
        if (n < 0) break;
        written += static_cast<size_t>(n);
    }
    offset_ += written;
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
    info.last_key = last_key_;

    // Set size unconditionally first, then override if compression is used
    info.handle.size = block_data.size();
    info.handle.is_compressed = false;

    // Try compression if enabled
    std::string compressed;
#ifdef HAVE_LZ4
    if (options_.compression == CompressionType::kLZ4Compression) {
        int max_compressed = LZ4_compressBound(static_cast<int>(block_data.size()));
        compressed.resize(sizeof(uint32_t) + max_compressed);
        // Store original size as 4-byte prefix for decompression
        EncodeFixed32(&compressed[0], static_cast<uint32_t>(block_data.size()));
        int compressed_size = LZ4_compress_default(
            block_data.data(), &compressed[sizeof(uint32_t)],
            static_cast<int>(block_data.size()), max_compressed);
        if (compressed_size > 0 &&
            static_cast<size_t>(compressed_size + sizeof(uint32_t)) < block_data.size()) {
            compressed.resize(sizeof(uint32_t) + compressed_size);
            info.handle.size = compressed.size();
            info.handle.is_compressed = true;
            writer_->Append(Slice(compressed));
        } else {
            // Compression didn't help, use original
            writer_->Append(block_data);
        }
    } else
#endif
    {
        writer_->Append(block_data);
    }

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
    std::string fname = writer_->filename();
    writer_->Close();
    std::remove(fname.c_str());
}

} // namespace lightkv