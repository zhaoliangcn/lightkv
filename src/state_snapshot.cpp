#include "lightkv/state_snapshot.h"

#include "lightkv/db.h"
#include "lightkv/options.h"
#include "lightkv/status.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace lightkv {
namespace {

constexpr char kMagic[4] = {'L', 'K', 'V', 'S'};
constexpr uint32_t kVersion = 1;

void AppendFixed32(std::string* out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void AppendFixed64(std::string* out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t ReadFixed32(const std::string& s, size_t* off) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(static_cast<uint8_t>(s[*off + i])) << (8 * i);
    }
    *off += 4;
    return v;
}

uint64_t ReadFixed64(const std::string& s, size_t* off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(s[*off + i])) << (8 * i);
    }
    *off += 8;
    return v;
}

}  // namespace

Status EncodeStateImage(DB* db, std::string* out) {
    if (!db) return Status::InvalidArgument("EncodeStateImage: db is null");
    if (!out) return Status::InvalidArgument("EncodeStateImage: out is null");
    out->clear();

    // DB::Scan has no cursor: every call restarts at SeekToFirst() and APPENDS
    // to `rows`. So it cannot be paged -- looping with a batch limit would
    // re-emit the same prefix over and over, and never terminate when the key
    // count is an exact multiple of the batch size. One pass, no limit games.
    // The image is built in memory regardless, so paging would not have saved
    // anything.
    std::vector<std::pair<std::string, std::string>> rows;
    ReadOptions ro;
    constexpr int kUnlimited = 0x7FFFFFFF;
    Status s = db->Scan(ro, "", kUnlimited, &rows);
    if (!s.ok()) return s;

    std::string buf;
    buf.append(kMagic, sizeof(kMagic));
    AppendFixed32(&buf, kVersion);
    AppendFixed64(&buf, static_cast<uint64_t>(rows.size()));
    for (const auto& row : rows) {
        AppendFixed32(&buf, static_cast<uint32_t>(row.first.size()));
        buf.append(row.first);
        AppendFixed32(&buf, static_cast<uint32_t>(row.second.size()));
        buf.append(row.second);
    }
    out->swap(buf);
    return Status::OK();
}

Status DecodeStateImage(const std::string& data,
                        std::vector<std::pair<std::string, std::string>>* rows) {
    if (!rows) return Status::InvalidArgument("DecodeStateImage: rows is null");
    rows->clear();

    constexpr size_t kHeaderSize = sizeof(kMagic) + 4 + 8;
    if (data.size() < kHeaderSize) {
        return Status::Corruption("snapshot image is truncated (" +
                                  std::to_string(data.size()) + " bytes)");
    }
    if (std::memcmp(data.data(), kMagic, sizeof(kMagic)) != 0) {
        return Status::Corruption("snapshot image has a bad magic");
    }

    size_t off = sizeof(kMagic);
    const uint32_t version = ReadFixed32(data, &off);
    if (version != kVersion) {
        return Status::Corruption("unsupported snapshot version " + std::to_string(version));
    }
    const uint64_t count = ReadFixed64(data, &off);

    // Guard against a count that cannot possibly fit in the remaining bytes
    // before reserving, so a corrupt header cannot drive a huge allocation.
    if (count > (data.size() - off) / 8 + 1) {
        return Status::Corruption("snapshot image declares " + std::to_string(count) +
                                  " entries but is only " + std::to_string(data.size()) +
                                  " bytes");
    }

    rows->reserve(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        if (off + 4 > data.size()) {
            return Status::Corruption("snapshot image truncated at key length");
        }
        const uint32_t klen = ReadFixed32(data, &off);
        if (off + klen + 4 > data.size()) {
            return Status::Corruption("snapshot image truncated at key body");
        }
        std::string key(data.data() + off, klen);
        off += klen;

        const uint32_t vlen = ReadFixed32(data, &off);
        if (off + vlen > data.size()) {
            return Status::Corruption("snapshot image truncated at value body");
        }
        std::string value(data.data() + off, vlen);
        off += vlen;

        rows->emplace_back(std::move(key), std::move(value));
    }

    if (off != data.size()) {
        return Status::Corruption("snapshot image has " + std::to_string(data.size() - off) +
                                  " bytes of trailing garbage");
    }
    return Status::OK();
}

Status RestoreStateImage(DB* db, const std::string& data) {
    if (!db) return Status::InvalidArgument("RestoreStateImage: db is null");

    // Decode fully before wiping anything. A truncated or corrupt payload must
    // leave the database serving its current state, not a half-restored one.
    std::vector<std::pair<std::string, std::string>> rows;
    Status s = DecodeStateImage(data, &rows);
    if (!s.ok()) return s;

    s = db->Clear();
    if (!s.ok()) return s;

    WriteOptions wo;
    wo.sync = false;
    for (const auto& row : rows) {
        s = db->Put(wo, row.first, row.second);
        if (!s.ok()) return s;
    }
    return Status::OK();
}

}  // namespace lightkv