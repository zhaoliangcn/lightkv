#include "lightkv/sstable.h"
#include "lightkv/encoding.h"
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#ifdef HAVE_LZ4
#include <lz4.h>
#endif

namespace lightkv {

void TableFooter::EncodeTo(std::string* dst) const {
    PutFixed64(dst, index_block_handle_offset);
    PutFixed64(dst, meta_index_block_handle_offset);
    dst->append("LIGHTKV1");
}

Status TableFooter::DecodeFrom(const Slice& input, TableFooter* footer) {
    // Footer format: [index_offset:8][meta_offset:8][magic:8] = 24 bytes
    if (input.size() < 24) return Status::Corruption("footer too short");
    const char* limit = input.data() + input.size();
    const char* magic = limit - 8;
    if (memcmp(magic, "LIGHTKV1", 8) != 0) {
        return Status::Corruption("bad footer magic");
    }
    footer->index_block_handle_offset = DecodeFixed64(input.data());
    footer->meta_index_block_handle_offset = DecodeFixed64(input.data() + 8);
    return Status::OK();
}

FileReader::FileReader(const std::string& filename)
    : filename_(filename), fd_(-1), file_size_(0), mmap_base_(nullptr) {}

FileReader::~FileReader() {
    if (mmap_base_ && mmap_base_ != MAP_FAILED) {
        ::munmap(mmap_base_, file_size_);
    }
    if (fd_ >= 0) ::close(fd_);
}

bool FileReader::Open() {
    fd_ = ::open(filename_.c_str(), O_RDONLY);
    if (fd_ < 0) return false;
    struct stat st;
    if (::fstat(fd_, &st) < 0) return false;
    file_size_ = static_cast<size_t>(st.st_size);
    if (file_size_ > 0) {
        mmap_base_ = ::mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (mmap_base_ == MAP_FAILED) return false;
    }
    return true;
}

Status FileReader::Read(uint64_t offset, size_t n, Slice* result, std::string* scratch) const {
    if (offset + n > file_size_) return Status::Corruption("read past end of file");
    if (mmap_base_) {
        *result = Slice(static_cast<const char*>(mmap_base_) + offset, n);
        return Status::OK();
    }
    scratch->resize(n);
    ssize_t rd = ::pread(fd_, &(*scratch)[0], n, static_cast<off_t>(offset));
    if (rd < 0 || static_cast<size_t>(rd) != n) return Status::IOError("read failed");
    *result = Slice(*scratch);
    return Status::OK();
}

SSTable::SSTable(const Options& options, const std::string& filename, uint64_t file_id)
    : options_(options), filename_(filename), file_id_(file_id),
      bloom_filter_(options.bloom_bits_per_key) {}

Status SSTable::Open() {
    reader_ = std::make_unique<FileReader>(filename_);
    if (!reader_->Open()) return Status::IOError("cannot open sstable file");

    std::string scratch;
    Slice footer_slice;
    auto s = reader_->Read(reader_->FileSize() - TableFooter::kEncodedLength,
                           TableFooter::kEncodedLength, &footer_slice, &scratch);
    if (!s.ok()) return s;

    TableFooter footer;
    s = TableFooter::DecodeFrom(footer_slice, &footer);
    if (!s.ok()) return s;

    // Read bloom filter
    uint64_t bloom_offset = footer.meta_index_block_handle_offset;
    uint64_t bloom_size = footer.index_block_handle_offset - bloom_offset;
    Slice bloom_slice;
    s = reader_->Read(bloom_offset, bloom_size, &bloom_slice, &scratch);
    if (s.ok() && bloom_slice.size() > 0) {
        size_t num_u32 = bloom_slice.size() / sizeof(uint32_t);
        const auto* bits = reinterpret_cast<const uint32_t*>(bloom_slice.data());
        bloom_filter_ = BloomFilter(bits, num_u32, options_.bloom_bits_per_key);
    }

    // Read index block
    uint64_t index_size = reader_->FileSize() - TableFooter::kEncodedLength - footer.index_block_handle_offset;
    Slice index_slice;
    s = reader_->Read(footer.index_block_handle_offset, index_size, &index_slice, &scratch);
    if (!s.ok()) return s;

    index_data_.assign(index_slice.data(), index_slice.size());
    index_block_ = Block(index_data_.data(), index_data_.size(), options_.paranoid_checks);

    auto iter = index_block_.NewIterator();
    iter.SeekToFirst();
    if (iter.Valid()) {
        smallest_key_ = iter.key().ToString();
    }
    while (iter.Valid()) {
        largest_key_ = iter.key().ToString();
        auto handle = BlockHandle::DecodeFrom(iter.value());
        data_block_handles_.push_back(handle);
        iter.Next();
    }

    return Status::OK();
}

bool SSTable::MayMatch(const Slice& key) const {
    return bloom_filter_.MayMatch(key);
}

Status SSTable::Get(const Slice& key, std::string* value, uint64_t* seq) const {
    if (!data_block_handles_.empty()) {
        if (!bloom_filter_.MayMatch(key)) {
            return Status::NotFound();
        }
    }

    auto iter = NewIterator();
    iter.Seek(key);
    if (iter.Valid() && iter.key() == key) {
        *value = iter.value().ToString();
        *seq = iter.seq();
        return Status::OK();
    }
    return Status::NotFound();
}

Status SSTable::ReadBlock(const BlockHandle& handle, BlockContents* result) const {
    std::string scratch;
    Slice block_slice;
    auto s = reader_->Read(handle.offset, handle.size, &block_slice, &scratch);
    if (!s.ok()) return s;
    
#ifdef HAVE_LZ4
    if (handle.is_compressed) {
        // Decompress the block
        // Original size is stored in the first 4 bytes of compressed data
        if (block_slice.size() < 4) {
            return Status::Corruption("compressed block too small");
        }
        uint32_t original_size = DecodeFixed32(block_slice.data());
        std::string decompressed;
        decompressed.resize(original_size);
        int decompressed_size = LZ4_decompress_safe(
            block_slice.data() + 4, &decompressed[0],
            static_cast<int>(block_slice.size()) - 4,
            static_cast<int>(original_size));
        if (decompressed_size != static_cast<int>(original_size)) {
            return Status::Corruption("failed to decompress block");
        }
        result->data = std::move(decompressed);
    } else
#endif
    {
        result->data.assign(block_slice.data(), block_slice.size());
    }
    return Status::OK();
}

// ========== SSTable::Iterator Implementation ==========

SSTable::Iterator::Iterator(const SSTable* table)
    : table_(table), data_block_index_(0), last_seq_(0) {}

bool SSTable::Iterator::Valid() const {
    return iter_ && iter_->Valid();
}

void SSTable::Iterator::SeekToFirst() {
    if (table_->data_block_handles_.empty()) {
        iter_.reset();
        return;
    }
    data_block_index_ = 0;
    SwitchToBlock(0);
    if (iter_) iter_->SeekToFirst();
}

void SSTable::Iterator::SeekToLast() {
    if (table_->data_block_handles_.empty()) {
        iter_.reset();
        return;
    }
    data_block_index_ = static_cast<int>(table_->data_block_handles_.size()) - 1;
    SwitchToBlock(data_block_index_);
    if (iter_) iter_->SeekToLast();
}

void SSTable::Iterator::Seek(const Slice& target) {
    if (table_->data_block_handles_.empty()) {
        iter_.reset();
        return;
    }

    // Binary search across data blocks using the index block
    auto idx_iter = table_->index_block_.NewIterator();
    idx_iter.Seek(target);

    int block_idx = -1;
    if (!idx_iter.Valid()) {
        // target > all index keys, check the last data block
        block_idx = static_cast<int>(table_->data_block_handles_.size()) - 1;
    } else {
        // Find which data block this index entry points to
        auto handle = BlockHandle::DecodeFrom(idx_iter.value());
        for (int i = 0; i < static_cast<int>(table_->data_block_handles_.size()); ++i) {
            const auto& h = table_->data_block_handles_[i];
            if (h.offset == handle.offset && h.size == handle.size) {
                block_idx = i;
                break;
            }
        }
        // Also check previous block since target might fall between blocks
        if (block_idx > 0) {
            // Peek at previous block's last key
            int prev_idx = block_idx - 1;
            SwitchToBlock(prev_idx);
            if (iter_) {
                iter_->SeekToLast();
                if (iter_->Valid() && iter_->key().compare(target) >= 0) {
                    // Target is in or after the previous block
                    block_idx = prev_idx;
                }
            }
        }
    }

    if (block_idx < 0) {
        iter_.reset();
        return;
    }

    data_block_index_ = block_idx;
    SwitchToBlock(data_block_index_);
    if (!iter_) return;

    iter_->Seek(target);

    // If not found in this block, try the next block
    if (!iter_->Valid() && data_block_index_ + 1 < static_cast<int>(table_->data_block_handles_.size())) {
        ++data_block_index_;
        SwitchToBlock(data_block_index_);
        if (iter_) iter_->SeekToFirst();
    }
}

void SSTable::Iterator::Next() {
    if (!Valid()) return;
    iter_->Next();
    if (!iter_->Valid()) {
        // Current block exhausted, move to next block
        ++data_block_index_;
        if (data_block_index_ < static_cast<int>(table_->data_block_handles_.size())) {
            SwitchToBlock(data_block_index_);
            if (iter_) {
                iter_->SeekToFirst();
                if (!iter_->Valid()) {
                    iter_.reset();
                }
            }
        } else {
            iter_.reset();
        }
    }
}

Slice SSTable::Iterator::key() const {
    return iter_ ? iter_->key() : Slice();
}

Slice SSTable::Iterator::value() const {
    return iter_ ? iter_->value() : Slice();
}

void SSTable::Iterator::SwitchToBlock(int index) {
    BlockContents contents;
    auto& handle = table_->data_block_handles_[index];
    auto s = table_->ReadBlock(handle, &contents);
    if (!s.ok()) {
        iter_.reset();
        block_.reset();
        return;
    }
    // Reset old block/iterator BEFORE moving new data to avoid use-after-free
    iter_.reset();
    block_.reset();
    block_data_ = std::move(contents.data);
    block_ = std::make_unique<Block>(block_data_.data(), block_data_.size(), table_->options_.paranoid_checks);
    iter_ = std::make_unique<Block::Iterator>(block_.get(), block_data_.data());
    iter_->SeekToFirst();
    if (!iter_->Valid()) {
        iter_.reset();
    }
}

} // namespace lightkv