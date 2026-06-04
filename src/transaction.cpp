#include "lightkv/transaction.h"
#include "lightkv/db_impl.h"

namespace lightkv {

Transaction::Transaction(DBImpl* db, uint64_t snapshot_seq)
    : db_(db), snapshot_seq_(snapshot_seq), committed_(false) {}

Transaction::~Transaction() {
    if (!committed_) {
        Rollback();
    }
}

Status Transaction::Put(const Slice& key, const Slice& value) {
    if (committed_) {
        return Status::InvalidArgument("transaction already committed");
    }
    write_buf_.push_back({WriteOp::kPut, key.ToString(), value.ToString()});
    return Status::OK();
}

Status Transaction::Delete(const Slice& key) {
    if (committed_) {
        return Status::InvalidArgument("transaction already committed");
    }
    write_buf_.push_back({WriteOp::kDelete, key.ToString(), ""});
    return Status::OK();
}

Status Transaction::Get(const Slice& key, std::string* value) {
    if (committed_) {
        return Status::InvalidArgument("transaction already committed");
    }

    // First check local write buffer (latest writes in transaction)
    for (auto it = write_buf_.rbegin(); it != write_buf_.rend(); ++it) {
        if (Slice(it->key) == key) {
            if (it->type == WriteOp::kDelete) {
                return Status::NotFound();
            }
            *value = it->value;
            return Status::OK();
        }
    }

    // Fall back to DB read at snapshot sequence
    ReadOptions ro;
    ro.snapshot_seq = snapshot_seq_;
    return db_->Get(ro, key, value);
}

Status Transaction::Commit() {
    if (committed_) {
        return Status::InvalidArgument("transaction already committed");
    }

    if (write_buf_.empty()) {
        committed_ = true;
        return Status::OK();
    }

    WriteOptions wo;
    wo.sync = true;

    for (const auto& op : write_buf_) {
        Status s;
        if (op.type == WriteOp::kPut) {
            s = db_->Put(wo, Slice(op.key), Slice(op.value));
        } else {
            s = db_->Delete(wo, Slice(op.key));
        }
        if (!s.ok()) {
            Rollback();
            return s;
        }
    }

    committed_ = true;
    write_buf_.clear();
    return Status::OK();
}

void Transaction::Rollback() {
    write_buf_.clear();
    committed_ = true;  // Mark as "done" to prevent further operations
}

} // namespace lightkv
