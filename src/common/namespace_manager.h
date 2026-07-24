#pragma once
#include <map>
#include <string>
#include <regex>

// 每个客户端连接跟踪当前命名空间
// 通过 conn->name() 字符串作为连接标识
//
// 内部 key 格式: {namespace}:{raw_key}
// RocksDB 中不同命名空间的 key 天然隔离

class NamespaceManager {
public:
    static constexpr const char* kDefaultNs = "default";
    static constexpr char        kSep      = ':';

    // 验证命名空间名称是否合法
    static bool IsValidName(const std::string& name) {
        if (name.empty() || name.size() > 63) return false;
        for (char c : name) {
            if (!isalnum(c) && c != '_' && c != '-') return false;
        }
        return true;
    }

    // 设置连接对应的命名空间
    void SetNs(const std::string& conn_name, const std::string& ns) {
        _ns[conn_name] = ns;
    }

    // 获取连接对应的命名空间（如果未设置过，返回默认值 "default"）
    std::string GetNs(const std::string& conn_name) const {
        auto it = _ns.find(conn_name);
        if (it != _ns.end()) return it->second;
        return kDefaultNs;
    }

    // 将原始 key 转换为内部存储 key: {namespace}:{raw_key}
    std::string MakeKey(const std::string& conn_name,
                        const std::string& raw_key) const {
        return GetNs(conn_name) + kSep + raw_key;
    }

    // 连接断开时清理映射
    void Remove(const std::string& conn_name) {
        _ns.erase(conn_name);
    }

private:
    std::map<std::string, std::string> _ns;
};
