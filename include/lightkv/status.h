#pragma once

#include <string>
#include <string_view>

namespace lightkv {

class Status {
public:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
    };

    Status() : code_(kOk) {}
    Status(Code code, std::string_view msg) : code_(code), msg_(msg) {}

    static Status OK() { return Status(); }
    static Status NotFound(std::string_view msg = "") { return Status(kNotFound, msg); }
    static Status Corruption(std::string_view msg = "") { return Status(kCorruption, msg); }
    static Status NotSupported(std::string_view msg = "") { return Status(kNotSupported, msg); }
    static Status InvalidArgument(std::string_view msg = "") { return Status(kInvalidArgument, msg); }
    static Status IOError(std::string_view msg = "") { return Status(kIOError, msg); }

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsCorruption() const { return code_ == kCorruption; }
    bool IsIOError() const { return code_ == kIOError; }

    Code code() const { return code_; }
    std::string_view message() const { return msg_; }
    std::string ToString() const;

private:
    Code code_;
    std::string msg_;
};

} // namespace lightkv