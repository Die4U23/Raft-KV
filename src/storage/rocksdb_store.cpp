#include "rocksdb_store.h"
#include <glog/logging.h>

RocksDBStore::RocksDBStore(const std::string& db_path) : _db(nullptr) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &_db);
    if (!s.ok()) {
        LOG(FATAL) << "Failed to open RocksDB at " << db_path << ": " << s.ToString();
    }
}

RocksDBStore::~RocksDBStore() {
    delete _db;
}

bool RocksDBStore::Put(const std::string& key, const std::string& value) {
    return _db->Put(rocksdb::WriteOptions(), key, value).ok();
}

bool RocksDBStore::Get(const std::string& key, std::string* value) const {
    return _db->Get(rocksdb::ReadOptions(), key, value).ok();
}

bool RocksDBStore::Delete(const std::string& key) {
    return _db->Delete(rocksdb::WriteOptions(), key).ok();
}
