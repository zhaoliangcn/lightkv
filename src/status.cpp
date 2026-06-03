#include "lightkv/status.h"
#include <string>

namespace lightkv {

std::string Status::ToString() const {
    if (code_ == kOk) return "OK";
    const char* type;
    switch (code_) {
        case kNotFound: type = "NotFound"; break;
        case kCorruption: type = "Corruption"; break;
        case kNotSupported: type = "NotSupported"; break;
        case kInvalidArgument: type = "InvalidArgument"; break;
        case kIOError: type = "IOError"; break;
        default: type = "Unknown"; break;
    }
    if (msg_.empty()) return std::string(type);
    return std::string(type) + ": " + msg_;
}

} // namespace lightkv