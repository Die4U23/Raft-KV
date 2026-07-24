#pragma once
#include <map>
#include <string>

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
    // 允许: [a-zA-Z0-9_-]+, 长度 1~63
    static bool IsValidName(const std::string& name);

    // 设置连接对应的命名空间
    void SetNs(const std::string& conn_name, const std::string& ns);

    // 获取连接对应的命名空间（未设置则返回 "default"）
    std::string GetNs(const std::string& conn_name) const;

    // 将原始 key 转换为带命名空间前缀的内部存储 key
    std::string MakeKey(const std::string& conn_name,
                        const std::string& raw_key) const;

    // 连接断开时清理映射
    void Remove(const std::string& conn_name);

private:
    std::map<std::string, std::string> _ns;
};
