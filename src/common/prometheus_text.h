#pragma once
// Prometheus text exposition (0.0.4) for the INFO field list.
// Cumulative fields are counters. Current values are gauges.
// `state` becomes raftkv_role. `namespace` is per RESP connection and is omitted.
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

enum class MetricsRequestKind { Incomplete, Ok, Health, NotFound, MethodNotAllowed, BadRequest };

inline bool EndsWith(std::string_view text, std::string_view suffix) {
    return text.size() >= suffix.size() &&
           text.substr(text.size() - suffix.size()) == suffix;
}

inline bool IsIntegerField(std::string_view text) {
    if (text.empty()) return false;
    size_t i = (text[0] == '-') ? 1 : 0;
    if (i == text.size()) return false;
    for (; i < text.size(); ++i)
        if (!std::isdigit(static_cast<unsigned char>(text[i]))) return false;
    return true;
}

inline bool IsCumulativeField(std::string_view key) {
    if (key == "queued_write_bytes" || key == "pending_proposal_bytes" ||
        key == "client_input_bytes" || key == "client_output_reserved_bytes")
        return false;
    if (EndsWith(key, "_count") || EndsWith(key, "_total_us") ||
        EndsWith(key, "_entries") || EndsWith(key, "_bytes"))
        return true;
    return key == "replication_retry_attempts" || key == "proposal_timeouts" ||
           key == "read_index_total" || key == "read_index_succeeded" ||
           key == "read_index_timeout" || key == "read_index_not_leader" ||
           key == "read_index_overload" || key == "read_index_lease" ||
           key == "overload_rejections" || key == "flush_immediate_scheduled" ||
           key == "flush_delayed_scheduled" || key == "flush_promotions" ||
           key == "proposal_batches" || key == "apply_batches";
}

inline const char* FieldHelp(std::string_view key) {
    if (key == "node_id") return "This process's Raft node id.";
    if (key == "leader_id") return "Current leader id, or -1 when this node does not know one.";
    if (key == "term") return "Current Raft term.";
    if (key == "commit_index") return "Highest log index known to be committed.";
    if (key == "last_applied") return "Highest log index applied to the state machine.";
    if (key == "apply_lag") return "Committed entries not yet applied (commit_index - last_applied).";
    if (key == "overload_rejections") return "Admission failures that rejected a request or closed a connection.";
    if (key == "snapshot_index") return "Last included index of the latest snapshot, or 0 when none exists.";
    if (EndsWith(key, "_total_us")) return "Sum of successful stage samples, in microseconds, since process start.";
    if (EndsWith(key, "_max_us")) return "Largest successful stage sample, in microseconds, since process start.";
    if (EndsWith(key, "_avg_us")) return "Integer-truncated mean of successful stage samples, in microseconds.";
    if (EndsWith(key, "_count")) return "Successful stage samples since process start.";
    return "INFO field exported for scraping.";
}

inline bool ValidMetricKey(std::string_view key) {
    if (key.empty() || !std::isalpha(static_cast<unsigned char>(key[0]))) return false;
    for (char ch : key)
        if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_') return false;
    return true;
}

inline std::string PrometheusText(std::string_view info) {
    static constexpr const char* kRoles[] = {
        "leader", "follower", "candidate", "pre-candidate", "stopped"};
    std::string out;
    size_t pos = 0;
    while (pos < info.size()) {
        size_t end = info.find('\n', pos);
        if (end == std::string_view::npos) end = info.size();
        std::string_view line = info.substr(pos, end - pos);
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        pos = end + (end < info.size() ? 1 : 0);
        const size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0) continue;
        const std::string_view key = line.substr(0, colon);
        const std::string_view value = line.substr(colon + 1);
        if (key == "namespace") continue;
        if (key == "state") {
            out += "# HELP raftkv_role Current Raft role. One known role is 1.\n";
            out += "# TYPE raftkv_role gauge\n";
            for (const char* role : kRoles) {
                out += "raftkv_role{role=\"";
                out += role;
                out += "\"} ";
                out += (value == role) ? "1" : "0";
                out += "\n";
            }
            continue;
        }
        if (!ValidMetricKey(key) || !IsIntegerField(value)) continue;
        const bool cumulative = IsCumulativeField(key);
        out += "# HELP raftkv_";
        out += key;
        out += " ";
        out += FieldHelp(key);
        out += "\n# TYPE raftkv_";
        out += key;
        out += cumulative ? " counter\n" : " gauge\n";
        out += "raftkv_";
        out += key;
        out += " ";
        out += value;
        out += "\n";
    }
    return out;
}

// First line of a complete HTTP/1.x request. Incomplete until the header block ends.
inline MetricsRequestKind ClassifyMetricsRequest(std::string_view raw) {
    if (raw.find("\r\n\r\n") == std::string_view::npos) return MetricsRequestKind::Incomplete;
    const size_t line_end = raw.find("\r\n");
    if (line_end == std::string_view::npos || line_end == 0) return MetricsRequestKind::BadRequest;
    const std::string_view line = raw.substr(0, line_end);
    const size_t method_end = line.find(' ');
    if (method_end == std::string_view::npos || method_end == 0) return MetricsRequestKind::BadRequest;
    const size_t path_end = line.find(' ', method_end + 1);
    if (path_end == std::string_view::npos) return MetricsRequestKind::BadRequest;
    const std::string_view method = line.substr(0, method_end);
    std::string_view target = line.substr(method_end + 1, path_end - method_end - 1);
    const std::string_view version = line.substr(path_end + 1);
    if (version != "HTTP/1.0" && version != "HTTP/1.1") return MetricsRequestKind::BadRequest;
    const size_t query = target.find('?');
    if (query != std::string_view::npos) target = target.substr(0, query);
    if (method != "GET") return MetricsRequestKind::MethodNotAllowed;
    if (target == "/health") return MetricsRequestKind::Health;
    if (target != "/metrics") return MetricsRequestKind::NotFound;
    return MetricsRequestKind::Ok;
}

// Process liveness for GET /health. `healthy` is the whole-process verdict that
// drives the HTTP status and the first line (a follower can be healthy; a
// stopped node is unhealthy even when storage is intact). `storage_healthy` is
// the narrower storage signal. Neither reports whether the node has a quorum.
inline std::string HealthText(int node_id, const char* state, int64_t term,
                              int leader_id, bool healthy, bool storage_healthy) {
    std::string out = std::string(healthy ? "ok\n" : "unavailable\n");
    out += "node_id:" + std::to_string(node_id) + "\n";
    out += "state:";
    out += state;
    out += "\nterm:" + std::to_string(term) + "\n";
    out += "leader_id:" + std::to_string(leader_id) + "\n";
    out += "storage_healthy:" + std::to_string(storage_healthy ? 1 : 0) + "\n";
    return out;
}

inline std::string MetricsHttpResponse(int status, const char* reason,
                                       std::string_view body, const char* content_type) {
    std::string out = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\n";
    out += "Content-Type: ";
    out += content_type;
    out += "\r\nContent-Length: ";
    out += std::to_string(body.size());
    out += "\r\nConnection: close\r\n\r\n";
    out += body;
    return out;
}
