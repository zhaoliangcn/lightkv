#pragma once

#include "slice.h"
#include "status.h"
#include "options.h"
#include <memory>
#include <string>
#include <vector>

namespace lightkv {

class DBImpl;

class Transaction {
public:
    Transaction(DBImpl* db, uint64_t snapshot_seq);
    ~Transaction();

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    Status Put(const Slice& key, const Slice& value);
    Status Delete(const Slice& key);
    Status Get(const Slice& key, std::string* value);

    Status Commit();
    void Rollback();

    bool IsCommitted() const { return committed_; }

private:
    struct WriteOp {
        enum Type { kPut, kDelete };
        Type type;
        std::string key;
        std::string value;
    };

    DBImpl* db_;
    uint64_t snapshot_seq_;
    bool committed_;
    std::vector<WriteOp> write_buf_;
};

} // namespace lightkv
