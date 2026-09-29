#include "common/prometheus_text.h"
#include <iostream>
#include <stdexcept>
#include <string>

static void Check(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

static void ExportsRoleTermProgressLagOverloadAndStageTime() {
    const std::string info =
        "node_id:1\r\n"
        "state:leader\r\n"
        "leader_id:-1\r\n"
        "term:4\r\n"
        "commit_index:9\r\n"
        "last_applied:7\r\n"
        "namespace:default\r\n"
        "apply_lag:2\r\n"
        "overload_rejections:3\r\n"
        "queued_write_bytes:8\r\n"
        "leader_log_write_count:5\r\n"
        "leader_log_write_total_us:50\r\n"
        "leader_log_write_max_us:20\r\n"
        "leader_log_write_avg_us:10\r\n"
        "not a field\r\n"
        "odd:nope\r\n";
    const std::string text = PrometheusText(info);
    Check(text.find("raftkv_role{role=\"leader\"} 1\n") != std::string::npos, "leader role");
    Check(text.find("raftkv_role{role=\"follower\"} 0\n") != std::string::npos, "other roles");
    Check(text.find("raftkv_role{role=\"pre-candidate\"} 0\n") != std::string::npos, "pre-candidate");
    Check(text.find("# TYPE raftkv_term gauge\nraftkv_term 4\n") != std::string::npos, "term");
    Check(text.find("# TYPE raftkv_commit_index gauge\nraftkv_commit_index 9\n") != std::string::npos,
          "commit");
    Check(text.find("# TYPE raftkv_last_applied gauge\nraftkv_last_applied 7\n") != std::string::npos,
          "applied");
    Check(text.find("# TYPE raftkv_apply_lag gauge\nraftkv_apply_lag 2\n") != std::string::npos, "lag");
    Check(text.find("# TYPE raftkv_overload_rejections counter\nraftkv_overload_rejections 3\n") !=
              std::string::npos,
          "overload");
    Check(text.find("# TYPE raftkv_leader_log_write_count counter\n") != std::string::npos, "stage count");
    Check(text.find("# TYPE raftkv_leader_log_write_total_us counter\n") != std::string::npos, "stage sum");
    Check(text.find("# TYPE raftkv_leader_log_write_max_us gauge\n") != std::string::npos, "stage max");
    Check(text.find("# TYPE raftkv_queued_write_bytes gauge\nraftkv_queued_write_bytes 8\n") !=
              std::string::npos,
          "queue bytes are a gauge");
    Check(text.find("raftkv_leader_id -1\n") != std::string::npos, "unknown leader");
    Check(text.find("namespace") == std::string::npos, "connection namespace was exported");
    Check(text.find("odd") == std::string::npos, "non-integer field was exported");
    Check(text.find("raftkv_role{role=\"stopped\"} 1\n") == std::string::npos, "stopped was current");
}

static void ClassifiesMetricsRequest() {
    Check(ClassifyMetricsRequest("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n") == MetricsRequestKind::Ok,
          "GET /metrics");
    Check(ClassifyMetricsRequest("GET /metrics?x=1 HTTP/1.0\r\n\r\n") == MetricsRequestKind::Ok,
          "query string");
    Check(ClassifyMetricsRequest("GET /metrics HTTP/1.1\r\n") == MetricsRequestKind::Incomplete,
          "headers not finished");
    Check(ClassifyMetricsRequest("POST /metrics HTTP/1.1\r\n\r\n") == MetricsRequestKind::MethodNotAllowed,
          "POST");
    Check(ClassifyMetricsRequest("GET /health HTTP/1.1\r\n\r\n") == MetricsRequestKind::NotFound,
          "other path");
    Check(ClassifyMetricsRequest("GET /metrics HTTP/2\r\n\r\n") == MetricsRequestKind::BadRequest,
          "bad version");
    const std::string body = "raftkv_term 1\n";
    const std::string response = MetricsHttpResponse(
        200, "OK", body, "text/plain; version=0.0.4; charset=utf-8");
    Check(response.find("HTTP/1.1 200 OK\r\n") == 0, "status");
    Check(response.find("Content-Length: " + std::to_string(body.size()) + "\r\n") != std::string::npos,
          "length");
    Check(response.size() >= 4 && response.compare(response.size() - body.size(), body.size(), body) == 0,
          "body");
}

int main() {
    ExportsRoleTermProgressLagOverloadAndStageTime();
    ClassifiesMetricsRequest();
    std::cout << "PASS: prometheus text for INFO fields\n";
}
