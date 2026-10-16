#include "lightkv/manifest.h"
#include "lightkv/encoding.h"
#include "lightkv/platform.h"
#include <cstdio>
#include <cstring>
#include <sstream>
#include <algorithm>

namespace lightkv {
namespace {

// Fold floor: even a tiny database should not keep an append log forever.
constexpr size_t kMinAppendRecords = 64;
// One appended record per this many live files. Folding writes one record per
// file, so folding on every few files would be a loss; folding only after the
// log has grown past the snapshot it replaces is what keeps replay bounded.
constexpr size_t kAppendRecordsPerFile = 4;
constexpr size_t kMaxAppendRecords = 1u << 20;  // sanity ceiling

}  // namespace

// Strict integer parsing: the manifest is the only record of which SSTables
// are live, so a corrupt field has to surface as a Status rather than as an
// exception thrown out of DB::Open. std::stoull would happily accept "", "-1"
// and "12abc" as 0.
bool ParseU64(const std::string& s, uint64_t* out) {
    if (s.empty()) return false;
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        uint64_t d = static_cast<uint64_t>(c - '0');
        if (v > (UINT64_MAX - d) / 10) return false;  // overflow
        v = v * 10 + d;
    }
    *out = v;
    return true;
}

bool ParseI32(const std::string& s, int32_t* out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
        if (s.size() == 1) return false;
    }
    int64_t v = 0;
    for (; i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
        v = v * 10 + (s[i] - '0');
        if (v > 2147483648LL) return false;
    }
    *out = static_cast<int32_t>(neg ? -v : v);
    return true;
}

namespace {

// Find "<label><number>" inside a line and parse the number strictly.
bool ParseU64After(const std::string& line, const char* label, uint64_t* out) {
    const size_t at = line.find(label);
    if (at == std::string::npos) return false;
    size_t start = at + strlen(label);
    size_t end = start;
    while (end < line.size() && line[end] >= '0' && line[end] <= '9') ++end;
    return ParseU64(line.substr(start, end - start), out);
}

// format_version 1: "  file: 3.sst, range: [aaa, ccc], size: 4096"
bool ParseLegacyFileLine(const std::string& line, FileMetaData* meta) {
    const size_t file_at = line.find("file: ");
    if (file_at == std::string::npos) return false;
    size_t i = file_at + 6;
    while (i < line.size() && line[i] >= '0' && line[i] <= '9') ++i;
    if (!ParseU64(line.substr(file_at + 6, i - (file_at + 6)), &meta->file_id)) {
        return false;
    }

    const size_t open = line.find('[', i);
    if (open == std::string::npos) return false;
    const size_t comma = line.find(',', open);
    if (comma == std::string::npos) return false;
    const size_t close = line.find(']', comma);
    if (close == std::string::npos) return false;
    meta->smallest_key = line.substr(open + 1, comma - open - 1);
    // The v1 writer always put exactly one space after the comma.
    size_t large_begin = comma + 1;
    if (large_begin < line.size() && line[large_begin] == ' ') ++large_begin;
    meta->largest_key = line.substr(large_begin, close - large_begin);

    uint64_t sz = 0;
    if (!ParseU64After(line, "size: ", &sz)) return false;
    meta->file_size = sz;
    return true;
}

}  // namespace

static std::vector<std::string> SplitString(const std::string& s, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream stream(s);
    while (std::getline(stream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

void Manifest::AddFile(int level, uint64_t file_id,
                       const std::string& smallest, const std::string& largest,
                       uint64_t file_size) {
    if (level < 0 || level >= 7) return;
    FileMetaData meta;
    meta.file_id = file_id;
    meta.level = level;
    meta.smallest_key = smallest;
    meta.largest_key = largest;
    meta.file_size = file_size;
    files[level].push_back(meta);
    RecordAdd(level, file_id, smallest, largest, file_size);
}

void Manifest::RemoveFile(int level, uint64_t file_id) {
    if (level < 0 || level >= 7) return;
    auto& level_files = files[level];
    level_files.erase(
        std::remove_if(level_files.begin(), level_files.end(),
                       [file_id](const FileMetaData& m) { return m.file_id == file_id; }),
        level_files.end());
    RecordRemove(level, file_id);
}

void Manifest::RecordAdd(int level, uint64_t file_id,
                         const std::string& smallest, const std::string& largest,
                         uint64_t file_size) {
    FileMetaData meta;
    meta.file_id = file_id;
    meta.level = level;
    meta.smallest_key = smallest;
    meta.largest_key = largest;
    meta.file_size = file_size;
    pending_adds_[level].push_back(meta);
}

void Manifest::RecordRemove(int level, uint64_t file_id) {
    pending_removals_.emplace_back(level, file_id);
}

// Removal that is NOT staged for the append log -- used while replaying it.
void Manifest::RemoveFileDirect(int level, uint64_t file_id) {
    if (level < 0 || level >= 7) return;
    auto& level_files = files[level];
    level_files.erase(
        std::remove_if(level_files.begin(), level_files.end(),
                       [file_id](const FileMetaData& m) { return m.file_id == file_id; }),
        level_files.end());
}

size_t Manifest::LiveFileCount() const {
    size_t n = 0;
    for (int i = 0; i < 7; ++i) n += files[i].size();
    return n;
}

size_t Manifest::FoldThreshold() const {
    const size_t scaled = LiveFileCount() / kAppendRecordsPerFile;
    size_t threshold = scaled > kMinAppendRecords ? scaled : kMinAppendRecords;
    return threshold > kMaxAppendRecords ? kMaxAppendRecords : threshold;
}

// 鈹€鈹€鈹€ Append-log framing 鈹€鈹€鈹€
//
// Keys are arbitrary byte strings. The snapshot format below spells them out as
// `range: [a, b]`, which cannot round-trip a key containing ',', ']' or a
// newline -- a comma in a key silently shifted every field after it on reload.
// Appended records therefore carry explicit lengths and raw bytes.
namespace {

constexpr char kAppendMagic[4] = {'L', 'M', 'V', '1'};
constexpr uint8_t kTagAdd = 1;
constexpr uint8_t kTagDel = 2;
constexpr uint8_t kTagSeq = 3;
constexpr uint8_t kTagSnapshot = 4;

void Put32(std::string* out, uint32_t v) {
    out->append(reinterpret_cast<const char*>(&v), 4);
}
void Put64(std::string* out, uint64_t v) {
    out->append(reinterpret_cast<const char*>(&v), 8);
}
void PutBytes(std::string* out, const std::string& s) {
    Put32(out, static_cast<uint32_t>(s.size()));
    out->append(s);
}

// Cursor over the append log. Bounds-checked on every read: a log truncated by
// a crash must return false rather than walk off the end of the buffer.
class Reader {
public:
    Reader(const char* p, size_t n) : p_(p), n_(n) {}

    bool Read8(uint8_t* v) {
        if (n_ - pos_ < 1) return false;
        *v = static_cast<uint8_t>(p_[pos_++]);
        return true;
    }
    bool Read32(uint32_t* v) {
        if (n_ - pos_ < 4) return false;
        ::memcpy(v, p_ + pos_, 4);
        pos_ += 4;
        return true;
    }
    bool Read64(uint64_t* v) {
        if (n_ - pos_ < 8) return false;
        ::memcpy(v, p_ + pos_, 8);
        pos_ += 8;
        return true;
    }
    bool ReadBytes(std::string* v) {
        uint32_t len = 0;
        if (!Read32(&len)) return false;
        if (n_ - pos_ < len) return false;
        v->assign(p_ + pos_, len);
        pos_ += len;
        return true;
    }

private:
    const char* p_;
    size_t n_;
    size_t pos_ = 0;
};

}  // namespace

Status Manifest::AppendChanges(const std::string& db_path) {
    const std::string manifest_path = db_path + "/MANIFEST";

    // Fold the log back into a snapshot once it has earned its cost.
    //
    // A snapshot costs one record per live SSTable, so a fixed record count is
    // the wrong yardstick: at 10k files, waiting for 512 appended records means
    // the snapshot could be written ~20x over before the fold pays for itself,
    // and the log just keeps growing. Scale the trigger with the file count
    // instead, with a floor so a tiny database still folds promptly.
    if (appended_records_ >= FoldThreshold()) {
        Status s = WriteToFile(db_path);
        if (!s.ok()) return s;  // keep the pending lists: they are still owed
        for (int i = 0; i < 7; ++i) pending_adds_[i].clear();
        pending_removals_.clear();
        appended_records_ = 0;
        return Status::OK();
    }

    // The magic belongs to the file, not to every frame. Repeating it per frame
    // made the second occurrence parse as a tag byte ('L' = 0x4C), which is not
    // a known tag, so a manifest with even one appended frame failed to reload.
    int fd = ::open(manifest_path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_BINARY, 0644);
    if (fd < 0) return Status::IOError("cannot open MANIFEST for append");

    struct stat st;
    if (::fstat(fd, &st) == 0 && st.st_size == 0) {
        // Only reachable if the very first update was an append (DBImpl writes a
        // snapshot first, but do not depend on that).
        const char* magic = kAppendMagic;
        ssize_t n = ::write(fd, magic, sizeof(kAppendMagic));
        if (n != static_cast<ssize_t>(sizeof(kAppendMagic))) {
            ::close(fd);
            return Status::IOError("failed to append to MANIFEST");
        }
    }

    std::string frame;
    frame.push_back(static_cast<char>(kTagSeq));
    Put64(&frame, last_seq);
    Put64(&frame, flushed_seq);
    Put64(&frame, next_file_id);

    // Removals first: replaying add-then-del vs del-then-add matters when a
    // file id is recycled, and deletions are the cheaper ordering to reason
    // about -- a delete always wins over an earlier add of the same id.
    for (const auto& rm : pending_removals_) {
        frame.push_back(static_cast<char>(kTagDel));
        Put32(&frame, static_cast<uint32_t>(rm.first));
        Put64(&frame, rm.second);
    }
    for (int level = 0; level < 7; ++level) {
        for (const auto& add : pending_adds_[level]) {
            frame.push_back(static_cast<char>(kTagAdd));
            Put32(&frame, static_cast<uint32_t>(level));
            Put64(&frame, add.file_id);
            Put64(&frame, add.file_size);
            PutBytes(&frame, add.smallest_key);
            PutBytes(&frame, add.largest_key);
        }
    }

    size_t total = 0;
    while (total < frame.size()) {
        ssize_t n = ::write(fd, frame.data() + total, frame.size() - total);
        if (n <= 0) {
            ::close(fd);
            return Status::IOError("failed to append to MANIFEST");
        }
        total += static_cast<size_t>(n);
    }
    ::fsync(fd);
    ::close(fd);

    // The counters and the pending lists are now on disk; the in-memory state
    // must forget them or the next call appends them a second time.
    for (int i = 0; i < 7; ++i) pending_adds_[i].clear();
    pending_removals_.clear();
    ++appended_records_;
    return Status::OK();
}

Status Manifest::WriteToFile(const std::string& db_path) const {
    std::string manifest_path = db_path + "/MANIFEST";
    std::string tmp_path = db_path + "/MANIFEST.tmp";
    std::string current_path = db_path + "/CURRENT";

    std::string content;
    content.append(kAppendMagic, sizeof(kAppendMagic));
    content.push_back(static_cast<char>(kTagSnapshot));
    Put64(&content, last_seq);
    Put64(&content, flushed_seq);
    Put64(&content, next_file_id);

    for (int level = 0; level < 7; ++level) {
        if (files[level].empty()) continue;
        Put32(&content, static_cast<uint32_t>(level));
        Put32(&content, static_cast<uint32_t>(files[level].size()));
        for (const auto& meta : files[level]) {
            Put64(&content, meta.file_id);
            Put64(&content, meta.file_size);
            PutBytes(&content, meta.smallest_key);
            PutBytes(&content, meta.largest_key);
        }
    }
    // End-of-levels marker. The level field cannot double as the terminator:
    // level 0 is a real level, and keying on it silently dropped every L0 file
    // (the reader stopped before them and then desynchronised on their bytes).
    Put32(&content, 0);  // level
    Put32(&content, 0);  // count == 0 terminates

    // O_BINARY is a no-op on POSIX but mandatory on Windows: without it the CRT
    // opens the file in text mode and translates every '\n' to "\r\n" on write.
    // That makes the returned byte count differ from content.size(), which trips
    // the short-write check below and silently aborts the manifest update.
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0644);
    if (fd < 0) return Status::IOError("cannot create MANIFEST.tmp");

    size_t total = 0;
    while (total < content.size()) {
        ssize_t n = ::write(fd, content.data() + total, content.size() - total);
        if (n <= 0) {
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return Status::IOError("failed to write MANIFEST");
        }
        total += static_cast<size_t>(n);
    }
    ::fsync(fd);
    ::close(fd);

    // Atomic rename: MANIFEST.tmp -> MANIFEST. This truncates the append log,
    // which is why the caller must drop its pending lists afterwards.
    if (platform_rename(tmp_path.c_str(), manifest_path.c_str()) < 0) {
        const int e = platform_errno();
        ::unlink(tmp_path.c_str());
        return Status::IOError("failed to rename MANIFEST (" +
                               std::to_string(e) + " " + platform_strerror(e) + ")");
    }

    // Write CURRENT file pointing to MANIFEST
    int cfd = ::open(current_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0644);
    if (cfd < 0) return Status::IOError("cannot create CURRENT");
    const char* current_content = "MANIFEST\n";
    ::write(cfd, current_content, strlen(current_content));
    ::fsync(cfd);
    ::close(cfd);

    return Status::OK();
}

Status Manifest::ReadFromFile(const std::string& db_path) {
    std::string manifest_path = db_path + "/MANIFEST";

    int fd = ::open(manifest_path.c_str(), O_RDONLY | O_BINARY);
    if (fd < 0) return Status::IOError("cannot open MANIFEST");

    struct stat st;
    if (::fstat(fd, &st) < 0) {
        ::close(fd);
        return Status::IOError("cannot stat MANIFEST");
    }

    std::string content;
    content.resize(static_cast<size_t>(st.st_size));
    ssize_t rd = st.st_size > 0 ? ::read(fd, &content[0], st.st_size) : 0;
    ::close(fd);

    if (rd < 0) return Status::IOError("failed to read MANIFEST");
    content.resize(static_cast<size_t>(rd));

    // Reloading into a reused Manifest must not stack duplicates on top of what
    // is already there.
    for (int i = 0; i < 7; ++i) files[i].clear();

    // Two on-disk shapes are accepted. Anything starting with the frame magic is
    // the current format (a snapshot followed by an append log); anything else is
    // the original text layout, which is still read so an existing directory
    // opens without a rebuild.
    if (content.size() >= sizeof(kAppendMagic) &&
        content.compare(0, sizeof(kAppendMagic), kAppendMagic, sizeof(kAppendMagic)) == 0) {
        const std::string log = content.substr(sizeof(kAppendMagic));
        if (!ReplayLog(log)) {
            return Status::Corruption("truncated MANIFEST");
        }
        appended_records_ = replayed_records_;
        return Status::OK();
    }

    auto lines = SplitString(content, '\n');
    bool in_files = false;
    int current_level = -1;

    for (const auto& line : lines) {
        if (line.empty()) continue;

        if (line == "---") {
            in_files = true;
            continue;
        }

        if (!in_files) {
            uint64_t v = 0;
            if (line.rfind("format_version: ", 0) == 0) {
                if (!ParseU64(line.substr(16), &v)) {
                    return Status::Corruption("bad format_version in MANIFEST");
                }
                format_version = static_cast<uint32_t>(v);
            } else if (line.rfind("last_seq: ", 0) == 0) {
                if (!ParseU64(line.substr(10), &last_seq)) {
                    return Status::Corruption("bad last_seq in MANIFEST");
                }
            } else if (line.rfind("flushed_seq: ", 0) == 0) {
                if (!ParseU64(line.substr(13), &flushed_seq)) {
                    return Status::Corruption("bad flushed_seq in MANIFEST");
                }
            } else if (line.rfind("next_file_id: ", 0) == 0) {
                if (!ParseU64(line.substr(14), &next_file_id)) {
                    return Status::Corruption("bad next_file_id in MANIFEST");
                }
            }
            continue;
        }

        if (line.rfind("Level ", 0) == 0 && line.back() == ':') {
            int32_t lvl = 0;
            if (!ParseI32(line.substr(6, line.size() - 7), &lvl)) {
                return Status::Corruption("bad level header in MANIFEST");
            }
            if (lvl < 0 || lvl >= 7) return Status::Corruption("level out of range");
            current_level = lvl;
            continue;
        }

        if (line.rfind("  file: ", 0) != 0 || current_level < 0) continue;

        FileMetaData meta;
        meta.level = current_level;
        if (!ParseLegacyFileLine(line, &meta)) {
            return Status::Corruption("bad file entry in MANIFEST");
        }
        files[current_level].push_back(meta);
    }

    return Status::OK();
}

bool Manifest::ReplayLog(const std::string& log) {
    Reader r(log.data(), log.size());
    replayed_records_ = 0;
    for (;;) {
        uint8_t tag = 0;
        if (!r.Read8(&tag)) break;  // clean end of log

        if (tag == kTagSnapshot) {
            // A snapshot resets the view: anything appended before it is
            // already folded in.
            for (int i = 0; i < 7; ++i) files[i].clear();
            if (!r.Read64(&last_seq) || !r.Read64(&flushed_seq) ||
                !r.Read64(&next_file_id)) {
                return false;
            }
            for (;;) {
                uint32_t level = 0;
                uint32_t count = 0;
                if (!r.Read32(&level) || !r.Read32(&count)) return false;
                if (count == 0) break;  // end-of-levels marker
                if (level >= 7) return false;
                for (uint32_t k = 0; k < count; ++k) {
                    FileMetaData meta;
                    if (!r.Read64(&meta.file_id) || !r.Read64(&meta.file_size) ||
                        !r.ReadBytes(&meta.smallest_key) ||
                        !r.ReadBytes(&meta.largest_key)) {
                        return false;
                    }
                    meta.level = static_cast<int>(level);
                    files[level].push_back(meta);
                }
            }
            continue;
        }

        ++replayed_records_;
        if (tag == kTagDel) {
            uint32_t level = 0;
            uint64_t file_id = 0;
            if (!r.Read32(&level) || !r.Read64(&file_id)) return false;
            if (level < 7) RemoveFileDirect(static_cast<int>(level), file_id);
        } else if (tag == kTagAdd) {
            uint32_t level = 0;
            uint64_t file_id = 0, file_size = 0;
            FileMetaData meta;
            if (!r.Read32(&level) || !r.Read64(&file_id) || !r.Read64(&file_size) ||
                !r.ReadBytes(&meta.smallest_key) || !r.ReadBytes(&meta.largest_key)) {
                return false;
            }
            if (level < 7) {
                meta.level = static_cast<int>(level);
                meta.file_id = file_id;
                meta.file_size = file_size;
                files[level].push_back(meta);
            }
        } else if (tag == kTagSeq) {
            if (!r.Read64(&last_seq) || !r.Read64(&flushed_seq) || !r.Read64(&next_file_id)) {
                return false;
            }
        } else {
            return false;  // unknown tag: the log is not what we think it is
        }
    }
    return true;
}

} // namespace lightkv
