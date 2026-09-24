#pragma once
#include "common/metrics.h"
#include <cstdint>
#include <string>
#include <unordered_map>

// Local cache of a versioned config value. Fresh() is false when the entry is
// missing or older than max_age. Callers must treat that as a miss and read
// the state machine (or surface the failure). This cache never returns a stale
// value as if it were fresh.
class ConfigCache {
public:
    void Store(const std::string& key, uint64_t version, const std::string& value,
               SteadyClock::time_point now) {
        _entries[key] = Entry{version, value, now};
    }
    void Invalidate(const std::string& key) { _entries.erase(key); }
    // max_age_ms < 0 is a miss. Age equal to max_age_ms is still fresh.
    bool Fresh(const std::string& key, SteadyClock::time_point now, int max_age_ms,
               uint64_t* version, std::string* value) const {
        if (max_age_ms < 0) return false;
        const auto found = _entries.find(key);
        if (found == _entries.end()) return false;
        if (now < found->second.stored_at) return false;
        const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - found->second.stored_at);
        if (age.count() > max_age_ms) return false;
        *version = found->second.version;
        *value = found->second.value;
        return true;
    }
private:
    struct Entry {
        uint64_t version = 0;
        std::string value;
        SteadyClock::time_point stored_at{};
    };
    std::unordered_map<std::string, Entry> _entries;
};
