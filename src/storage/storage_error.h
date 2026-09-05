#pragma once
#include <rocksdb/db.h>
#include <stdexcept>
#include <string>

inline void RequireStorageOK(const rocksdb::Status& status, const char* operation) {
    if (!status.ok())
        throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}
inline rocksdb::WriteOptions DurableWriteOptions() {
    rocksdb::WriteOptions options;
    options.sync = true;
    return options;
}
