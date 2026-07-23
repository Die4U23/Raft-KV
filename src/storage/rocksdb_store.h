#pragma once
#include <rocksdb/db.h>
#include <string>

class RocksDBStore {
public:
    RocksDBStore(const std::string& db_path);
    ~RocksDBStore();

    bool Put(const std::string& key, const std::string& value);
    bool Get(const std::string& key, std::string* value) const;
    bool Delete(const std::string& key);

private:
    rocksdb::DB* _db;
};
