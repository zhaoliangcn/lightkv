#include "lightkv/manifest.h"
#include "lightkv/encoding.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <algorithm>

namespace lightkv {

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
}

void Manifest::RemoveFile(int level, uint64_t file_id) {
    if (level < 0 || level >= 7) return;
    auto& level_files = files[level];
    level_files.erase(
        std::remove_if(level_files.begin(), level_files.end(),
                       [file_id](const FileMetaData& m) { return m.file_id == file_id; }),
        level_files.end());
}

Status Manifest::WriteToFile(const std::string& db_path) const {
    std::string manifest_path = db_path + "/MANIFEST";
    std::string tmp_path = db_path + "/MANIFEST.tmp";
    std::string current_path = db_path + "/CURRENT";

    // Write to temporary file first
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return Status::IOError("cannot create MANIFEST.tmp");

    std::string content;
    content += "format_version: " + std::to_string(format_version) + "\n";
    content += "last_seq: " + std::to_string(last_seq) + "\n";
    content += "flushed_seq: " + std::to_string(flushed_seq) + "\n";
    content += "next_file_id: " + std::to_string(next_file_id) + "\n";
    content += "---\n";

    for (int level = 0; level < 7; ++level) {
        if (files[level].empty()) continue;
        content += "Level " + std::to_string(level) + ":\n";
        for (const auto& meta : files[level]) {
            std::ostringstream oss;
            oss << "  file: " << meta.file_id << ".sst, range: ["
                << meta.smallest_key << ", " << meta.largest_key
                << "], size: " << meta.file_size << "\n";
            content += oss.str();
        }
    }

    ssize_t written = ::write(fd, content.data(), content.size());
    ::fsync(fd);
    ::close(fd);

    if (static_cast<size_t>(written) != content.size()) {
        ::unlink(tmp_path.c_str());
        return Status::IOError("failed to write MANIFEST");
    }

    // Atomic rename: MANIFEST.tmp -> MANIFEST
    if (::rename(tmp_path.c_str(), manifest_path.c_str()) < 0) {
        ::unlink(tmp_path.c_str());
        return Status::IOError("failed to rename MANIFEST");
    }

    // Write CURRENT file pointing to MANIFEST
    int cfd = ::open(current_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (cfd < 0) return Status::IOError("cannot create CURRENT");
    const char* current_content = "MANIFEST\n";
    ::write(cfd, current_content, strlen(current_content));
    ::fsync(cfd);
    ::close(cfd);

    return Status::OK();
}

Status Manifest::ReadFromFile(const std::string& db_path) {
    std::string manifest_path = db_path + "/MANIFEST";

    int fd = ::open(manifest_path.c_str(), O_RDONLY);
    if (fd < 0) return Status::IOError("cannot open MANIFEST");

    struct stat st;
    if (::fstat(fd, &st) < 0) {
        ::close(fd);
        return Status::IOError("cannot stat MANIFEST");
    }

    std::string content;
    content.resize(st.st_size);
    ssize_t rd = ::read(fd, &content[0], st.st_size);
    ::close(fd);

    if (rd < 0) return Status::IOError("failed to read MANIFEST");
    content.resize(rd);

    // Parse manifest
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
            // Parse header fields
            if (line.rfind("format_version: ", 0) == 0) {
                format_version = static_cast<uint32_t>(
                    std::stoull(line.substr(16)));
            } else if (line.rfind("last_seq: ", 0) == 0) {
                last_seq = std::stoull(line.substr(10));
            } else if (line.rfind("flushed_seq: ", 0) == 0) {
                flushed_seq = std::stoull(line.substr(13));
            } else if (line.rfind("next_file_id: ", 0) == 0) {
                next_file_id = std::stoull(line.substr(14));
            }
        } else {
            // Parse file entries
            if (line.rfind("Level ", 0) == 0 && line.back() == ':') {
                current_level = std::stoi(line.substr(6, line.size() - 7));
            } else if (line.rfind("  file: ", 0) == 0 && current_level >= 0) {
                // Parse: file: 1.sst, range: [aaa, ccc], size: 4096
                FileMetaData meta;
                meta.level = current_level;

                // Extract file_id
                size_t colon = line.find('.', 7);
                if (colon != std::string::npos) {
                    meta.file_id = std::stoull(line.substr(7, colon - 7));
                }

                // Extract range
                size_t range_start = line.find("[", 0);
                size_t comma = line.find(",", range_start);
                size_t range_end = line.find("]", comma);
                if (range_start != std::string::npos && comma != std::string::npos && range_end != std::string::npos) {
                    meta.smallest_key = line.substr(range_start + 1, comma - range_start - 1);
                    meta.largest_key = line.substr(comma + 2, range_end - comma - 2);
                }

                // Extract size
                size_t size_pos = line.find("size: ", 0);
                if (size_pos != std::string::npos) {
                    meta.file_size = std::stoull(line.substr(size_pos + 6));
                }

                files[current_level].push_back(meta);
            }
        }
    }

    return Status::OK();
}

} // namespace lightkv
