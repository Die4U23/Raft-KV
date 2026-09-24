#pragma once
#include "common/resp_parser.h"
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Joint configuration carried in the log and in a side key of the Raft log.
// commit_appended is reconstructed from the unapplied suffix and is not stored.

struct PeerEndpoint {
    std::string host;
    int port = 0;
};

struct MembershipState {
    std::set<int> voters;
    bool joint = false;
    std::set<int> next;
    int64_t change_index = 0;
    bool commit_appended = false;
    // Peers learned after process start. Absent for the static peer list.
    std::map<int, PeerEndpoint> endpoints;
};

enum class MemberKind { Join, Leave, Commit };

struct MemberCommand {
    MemberKind kind = MemberKind::Commit;
    int peer = -1;
    // Set only for JOIN of a peer that was not in the static list.
    std::string host;
    int port = 0;
};

inline void AppendU32Be(std::string* out, uint32_t value) {
    char bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->append(bytes, 4);
}
inline void AppendU64Be(std::string* out, uint64_t value) {
    char bytes[8];
    for (int i = 7; i >= 0; --i) {
        bytes[i] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    out->append(bytes, 8);
}
inline bool ReadU32Be(const std::string& in, size_t* offset, uint32_t* value) {
    if (*offset > in.size() || in.size() - *offset < 4) return false;
    uint32_t parsed = 0;
    for (int i = 0; i < 4; ++i)
        parsed = (parsed << 8) | static_cast<uint8_t>(in[*offset + static_cast<size_t>(i)]);
    *offset += 4;
    *value = parsed;
    return true;
}
inline bool ReadU64Be(const std::string& in, size_t* offset, uint64_t* value) {
    if (*offset > in.size() || in.size() - *offset < 8) return false;
    uint64_t parsed = 0;
    for (int i = 0; i < 8; ++i)
        parsed = (parsed << 8) | static_cast<uint8_t>(in[*offset + static_cast<size_t>(i)]);
    *offset += 8;
    *value = parsed;
    return true;
}

inline bool ValidEndpoint(const std::string& host, int port) {
    if (host.empty() || host.size() > 253 || host.find('\0') != std::string::npos) return false;
    return port >= 1 && port <= 65535;
}

inline std::string EncodeMembership(const MembershipState& state) {
    if (state.voters.empty()) throw std::runtime_error("empty voter set");
    std::string out;
    out.push_back(state.endpoints.empty() ? 1 : 2);
    out.push_back(state.joint ? 1 : 0);
    AppendU32Be(&out, static_cast<uint32_t>(state.voters.size()));
    for (int id : state.voters) AppendU32Be(&out, static_cast<uint32_t>(id));
    AppendU32Be(&out, static_cast<uint32_t>(state.next.size()));
    for (int id : state.next) AppendU32Be(&out, static_cast<uint32_t>(id));
    AppendU64Be(&out, static_cast<uint64_t>(state.change_index));
    if (!state.endpoints.empty()) {
        AppendU32Be(&out, static_cast<uint32_t>(state.endpoints.size()));
        for (const auto& item : state.endpoints) {
            if (!ValidEndpoint(item.second.host, item.second.port))
                throw std::runtime_error("invalid learned peer");
            AppendU32Be(&out, static_cast<uint32_t>(item.first));
            AppendU32Be(&out, static_cast<uint32_t>(item.second.port));
            AppendU32Be(&out, static_cast<uint32_t>(item.second.host.size()));
            out.append(item.second.host);
        }
    }
    return out;
}

inline bool DecodeMembership(const std::string& raw, MembershipState* state) {
    if (raw.size() < 2 || (raw[0] != 1 && raw[0] != 2)) return false;
    MembershipState parsed;
    parsed.joint = raw[1] == 1;
    if (raw[1] != 0 && raw[1] != 1) return false;
    size_t offset = 2;
    auto read_ids = [&](std::set<int>* ids) {
        uint32_t count = 0;
        if (!ReadU32Be(raw, &offset, &count) || (count == 0 && ids == &parsed.voters)) return false;
        if (ids == &parsed.next && !parsed.joint && count != 0) return false;
        if (ids == &parsed.next && parsed.joint && count == 0) return false;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t id = 0;
            if (!ReadU32Be(raw, &offset, &id) || id > static_cast<uint32_t>(INT32_MAX)) return false;
            if (!ids->insert(static_cast<int>(id)).second) return false;
        }
        return true;
    };
    if (!read_ids(&parsed.voters) || parsed.voters.empty()) return false;
    if (!read_ids(&parsed.next)) return false;
    uint64_t change = 0;
    if (!ReadU64Be(raw, &offset, &change)) return false;
    if (change > static_cast<uint64_t>(INT64_MAX)) return false;
    parsed.change_index = static_cast<int64_t>(change);
    if (parsed.joint && parsed.change_index <= 0) return false;
    if (!parsed.joint && parsed.change_index != 0) return false;
    if (raw[0] == 2) {
        uint32_t count = 0;
        if (!ReadU32Be(raw, &offset, &count) || count == 0) return false;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t id = 0, port = 0, host_len = 0;
            if (!ReadU32Be(raw, &offset, &id) || !ReadU32Be(raw, &offset, &port) ||
                !ReadU32Be(raw, &offset, &host_len) ||
                offset > raw.size() || raw.size() - offset < host_len)
                return false;
            PeerEndpoint endpoint;
            endpoint.host = raw.substr(offset, host_len);
            offset += host_len;
            endpoint.port = static_cast<int>(port);
            if (id > static_cast<uint32_t>(INT32_MAX) ||
                !ValidEndpoint(endpoint.host, endpoint.port) ||
                !parsed.endpoints.emplace(static_cast<int>(id), endpoint).second)
                return false;
        }
    }
    if (offset != raw.size()) return false;
    *state = std::move(parsed);
    return true;
}

// False means this command is not a membership entry. True with ok false means
// the verb was MEMBER but the arguments cannot be applied.
inline bool ParseMemberCommand(const std::string& command, MemberCommand* out, bool* ok) {
    *ok = false;
    if (command.empty()) return false;
    const auto parsed = RespParser::TryParseOne(command);
    if (parsed.state != RespParser::State::Complete || parsed.consumed != command.size())
        return false;
    if (parsed.args.empty()) return false;
    std::string op = parsed.args[0];
    for (char& c : op) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (op != "MEMBER") return false;
    auto peer_id = [](const std::string& text, int* id) {
        if (text.empty() || text.size() > 10 || (text.size() > 1 && text[0] == '0')) return false;
        for (char c : text) if (c < '0' || c > '9') return false;
        int value = 0;
        const auto converted = std::from_chars(text.data(), text.data() + text.size(), value);
        if (converted.ec != std::errc{} || converted.ptr != text.data() + text.size() || value < 0)
            return false;
        *id = value;
        return true;
    };
    if (parsed.args.size() == 2 && parsed.args[1] == "COMMIT") {
        out->kind = MemberKind::Commit;
        out->peer = -1;
        *ok = true;
        return true;
    }
    if (parsed.args.size() == 3 && (parsed.args[1] == "JOIN" || parsed.args[1] == "LEAVE") &&
        peer_id(parsed.args[2], &out->peer)) {
        out->kind = parsed.args[1] == "JOIN" ? MemberKind::Join : MemberKind::Leave;
        *ok = true;
        return true;
    }
    if (parsed.args.size() == 5 && parsed.args[1] == "JOIN" &&
        peer_id(parsed.args[2], &out->peer)) {
        int port = 0;
        if (!peer_id(parsed.args[4], &port) || !ValidEndpoint(parsed.args[3], port)) {
            *ok = false;
            return true;
        }
        out->kind = MemberKind::Join;
        out->host = parsed.args[3];
        out->port = port;
        *ok = true;
        return true;
    }
    *ok = false;
    return true;
}

inline std::string MemberCommandText(MemberKind kind, int peer, const std::string& host = {},
                                     int port = 0) {
    std::vector<std::string> args = {"MEMBER"};
    if (kind == MemberKind::Commit) args.push_back("COMMIT");
    else {
        args.push_back(kind == MemberKind::Join ? "JOIN" : "LEAVE");
        args.push_back(std::to_string(peer));
        if (!host.empty()) {
            args.push_back(host);
            args.push_back(std::to_string(port));
        }
    }
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

inline std::string EncodeMemberForward(uint64_t rpc, bool join, int peer, const std::string& host,
                                       int port) {
    std::string out(1, 'Q');
    AppendU64Be(&out, rpc);
    out.push_back(join ? 1 : 0);
    AppendU32Be(&out, static_cast<uint32_t>(peer));
    AppendU32Be(&out, static_cast<uint32_t>(port));
    AppendU32Be(&out, static_cast<uint32_t>(host.size()));
    out.append(host);
    return out;
}
inline bool DecodeMemberForward(const std::string& raw, uint64_t* rpc, bool* join, int* peer,
                                std::string* host, int* port) {
    if (raw.size() < 1 + 8 + 1 + 12 || raw[0] != 'Q') return false;
    size_t offset = 1;
    uint64_t parsed_rpc = 0;
    if (!ReadU64Be(raw, &offset, &parsed_rpc) || parsed_rpc == 0) return false;
    const bool parsed_join = raw[offset] == 1;
    if (raw[offset] != 0 && raw[offset] != 1) return false;
    ++offset;
    uint32_t parsed_peer = 0, parsed_port = 0, host_len = 0;
    if (!ReadU32Be(raw, &offset, &parsed_peer) || !ReadU32Be(raw, &offset, &parsed_port) ||
        !ReadU32Be(raw, &offset, &host_len) || raw.size() - offset != host_len)
        return false;
    if (parsed_peer > static_cast<uint32_t>(INT32_MAX)) return false;
    *rpc = parsed_rpc;
    *join = parsed_join;
    *peer = static_cast<int>(parsed_peer);
    *port = static_cast<int>(parsed_port);
    host->assign(raw, offset, host_len);
    return true;
}
inline std::string EncodeMemberForwardReply(uint64_t rpc, bool ok, const std::string& reply) {
    std::string out(1, 'A');
    AppendU64Be(&out, rpc);
    out.push_back(ok ? 1 : 0);
    AppendU32Be(&out, static_cast<uint32_t>(reply.size()));
    out.append(reply);
    return out;
}
inline bool DecodeMemberForwardReply(const std::string& raw, uint64_t* rpc, bool* ok,
                                     std::string* reply) {
    if (raw.size() < 1 + 8 + 1 + 4 || raw[0] != 'A') return false;
    size_t offset = 1;
    uint64_t parsed_rpc = 0;
    if (!ReadU64Be(raw, &offset, &parsed_rpc) || parsed_rpc == 0) return false;
    if (raw[offset] != 0 && raw[offset] != 1) return false;
    const bool parsed_ok = raw[offset] == 1;
    ++offset;
    uint32_t length = 0;
    if (!ReadU32Be(raw, &offset, &length) || raw.size() - offset != length) return false;
    *rpc = parsed_rpc;
    *ok = parsed_ok;
    reply->assign(raw, offset, length);
    return true;
}
