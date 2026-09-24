#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

// FNV-1a. shards <= 1 always selects the only group, so a one-shard process
// does not hash and does not change key placement.
inline int ShardOf(const std::string& key, int shards) {
    if (shards <= 1) return 0;
    uint32_t hash = 2166136261u;
    for (unsigned char byte : key) {
        hash ^= byte;
        hash *= 16777619u;
    }
    return static_cast<int>(hash % static_cast<uint32_t>(shards));
}

// Prepended to a Raft payload only when a process hosts more than one group.
// A single group leaves the protobuf bytes unchanged.
inline std::string PrefixShardPayload(int shard_count, int shard, const std::string& payload) {
    if (shard_count <= 1) return payload;
    if (shard < 0 || shard >= shard_count) throw std::invalid_argument("raft shard out of range");
    std::string out(4, '\0');
    auto value = static_cast<uint32_t>(shard);
    for (int i = 3; i >= 0; --i) {
        out[static_cast<size_t>(i)] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out.append(payload);
    return out;
}
inline bool StripShardPayload(int shard_count, const std::string& in, int* shard,
                              std::string* payload) {
    if (shard_count <= 1) {
        *shard = 0;
        *payload = in;
        return true;
    }
    if (in.size() < 4) return false;
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i)
        value = (value << 8) | static_cast<uint8_t>(in[static_cast<size_t>(i)]);
    if (value >= static_cast<uint32_t>(shard_count)) return false;
    *shard = static_cast<int>(value);
    *payload = in.substr(4);
    return true;
}
