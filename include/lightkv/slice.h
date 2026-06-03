#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <algorithm>

namespace lightkv {

class Slice {
public:
    Slice() : data_(""), size_(0) {}

    Slice(const char* data, size_t size) : data_(data), size_(size) {}

    Slice(const char* str) : data_(str), size_(strlen(str)) {}

    Slice(const std::string& str) : data_(str.data()), size_(str.size()) {}

    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    const char* begin() const { return data_; }
    const char* end() const { return data_ + size_; }

    char operator[](size_t i) const { return data_[i]; }

    std::string ToString() const { return std::string(data_, size_); }
    std::string_view ToStringView() const { return std::string_view(data_, size_); }

    bool operator==(const Slice& other) const {
        if (size_ != other.size_) return false;
        return memcmp(data_, other.data_, size_) == 0;
    }

    bool operator!=(const Slice& other) const { return !(*this == other); }

    int compare(const Slice& other) const {
        size_t min_len = std::min(size_, other.size_);
        int cmp = memcmp(data_, other.data_, min_len);
        if (cmp != 0) return cmp;
        if (size_ < other.size_) return -1;
        if (size_ > other.size_) return 1;
        return 0;
    }

    bool operator<(const Slice& other) const { return compare(other) < 0; }
    bool operator>(const Slice& other) const { return compare(other) > 0; }

    void remove_prefix(size_t n) {
        data_ += n;
        size_ -= n;
    }

    uint32_t Hash() const;

private:
    const char* data_;
    size_t size_;
};

inline uint32_t Slice::Hash() const {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < size_; ++i) {
        h ^= static_cast<uint8_t>(data_[i]);
        h *= 0x01000193;
    }
    return h;
}

struct SliceHash {
    size_t operator()(const Slice& s) const { return s.Hash(); }
};

struct SliceEqual {
    bool operator()(const Slice& a, const Slice& b) const { return a == b; }
};

inline std::string operator+(const Slice& a, const Slice& b) {
    std::string result;
    result.reserve(a.size() + b.size());
    result.append(a.data(), a.size());
    result.append(b.data(), b.size());
    return result;
}

inline std::string operator+(const std::string& a, const Slice& b) {
    std::string result = a;
    result.append(b.data(), b.size());
    return result;
}

inline std::string operator+(const Slice& a, const std::string& b) {
    std::string result(a.data(), a.size());
    result.append(b);
    return result;
}

} // namespace lightkv