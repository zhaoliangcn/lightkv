#pragma once

// Raft state machine snapshots.
//
// The payload is deliberately NOT a copy of the database directory:
//
//   - the directory is not self-describing, and restoring it would mean
//     replacing the DB object underneath a running Server (99 handlers hold a
//     raw DB*);
//   - a full image of the live key space restores through DB::Clear() plus a
//     bulk write, which needs no such surgery;
//   - the image is complete, so installing it on a follower that never saw the
//     earlier entries yields exactly the leader's state.
//
// Range tombstones deliberately do NOT travel: after Clear() there is nothing
// left for them to shadow, and the image already omits every key they deleted.
// TTL metadata does travel, because it lives in the DB itself under
// ttl_key() and is therefore part of the key space.
//
// Format: "LKVS" | version(4) | count(8) | count * [klen(4) key vlen(4) value]
// All integers are little-endian. Decoding validates the entire payload before
// any of it is applied, so a truncated or corrupt image leaves the database
// untouched.

#include "db.h"
#include "status.h"
#include <string>
#include <utility>
#include <vector>

namespace lightkv {

// Serialize every live key/value pair in `db`.
// Returns Corruption-free OK with a well-formed image; OK with an empty string
// is not a valid image, so callers must check `out->empty()`.
Status EncodeStateImage(DB* db, std::string* out);

// Parse an image produced by EncodeStateImage(). Validates magic, version,
// every length prefix and that there is no trailing data.
Status DecodeStateImage(const std::string& data,
                        std::vector<std::pair<std::string, std::string>>* rows);

// Decode + replace the contents of `db` with the image.
Status RestoreStateImage(DB* db, const std::string& data);

}  // namespace lightkv