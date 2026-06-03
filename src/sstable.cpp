#include "lightkv/sstable.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>

namespace lightkv {

void TableFooter::EncodeTo(std::string* dst) const {
    PutFixed64(dst, index_block_handle_offset);
    PutFixed64(dst, meta_index_block_handle_offset);
    dst->append("LIGHTKV1");
}

Status TableFooter::DecodeFrom(const Slice& input, TableFooter* footer) {
    const char* p = input.data();
    const char* limit = input.data() + input.size();
    if (input.size() < 8) return Status::Corruption("footer too short");
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
    index_block_ = Block(index_data_.data(), index_data_.size());

    auto iter = index_block_.NewIterator();
    iter.SeekToFirst();
    while (iter.Valid()) {
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

    auto idx_iter = index_block_.NewIterator();
    idx_iter.Seek(key);
    if (!idx_iter.Valid()) {

        if (!data_block_handles_.empty()) {
            const auto& handle = data_block_handles_.back();
            std::string scratch;
            Slice block_slice;
            auto s = reader_->Read(handle.offset, handle.size, &block_slice, &scratch);
            if (!s.ok()) return s;
            Block data_block(block_slice.data(), block_slice.size());
            auto iter = data_block.NewIterator();
            iter.Seek(key);
            if (iter.Valid() && iter.key() == key) {
                *value = iter.value().ToString();
                *seq = 0;
                return Status::OK();
            }
        }
        return Status::NotFound();
    }

    auto handle = BlockHandle::DecodeFrom(idx_iter.value());

    std::string scratch;
    Slice block_slice;
    auto s = reader_->Read(handle.offset, handle.size, &block_slice, &scratch);
    if (!s.ok()) return s;

    Block data_block(block_slice.data(), block_slice.size());
    auto iter = data_block.NewIterator();
    iter.Seek(key);
    if (iter.Valid() && iter.key() == key) {
        *value = iter.value().ToString();
        *seq = 0;
        return Status::OK();
    }

    return Status::NotFound();
}

} // namespace lightkv