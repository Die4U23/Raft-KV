#include "namespace/namespace_manager.h"

bool NamespaceManager::IsValidName(const std::string& name) {
    if (name.empty() || name.size() > 63) return false;
    for (char c : name) {
        if (!isalnum(c) && c != '_' && c != '-') return false;
    }
    return true;
}

void NamespaceManager::SetNs(const std::string& conn_name,
                              const std::string& ns) {
    _ns[conn_name] = ns;
}

std::string NamespaceManager::GetNs(const std::string& conn_name) const {
    auto it = _ns.find(conn_name);
    if (it != _ns.end()) return it->second;
    return kDefaultNs;
}

std::string NamespaceManager::MakeKey(const std::string& conn_name,
                                       const std::string& raw_key) const {
    return GetNs(conn_name) + kSep + raw_key;
}

void NamespaceManager::Remove(const std::string& conn_name) {
    _ns.erase(conn_name);
}
