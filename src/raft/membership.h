#pragma once
#include "common/resp_parser.h"
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Joint configuration carried in the log and in a side key of the Raft log.
// commit_appended is reconstructed from the unapplied suffix and is not stored.

struct MembershipState {
    std::set<int> voters;
    bool joint = false;
    std::set<int> next;
    int64_t change_index = 0;
    bool commit_appended = false;
};

enum class MemberKind { Join, Leave, Commit };

struct MemberCommand {
    MemberKind kind = MemberKind::Commit;
    int peer = -1;
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

inline std::string EncodeMembership(const MembershipState& state) {
    if (state.voters.empty()) throw std::runtime_error("empty voter set");
    std::string out;
    out.push_back(1);
    out.push_back(state.joint ? 1 : 0);
    AppendU32Be(&out, static_cast<uint32_t>(state.voters.size()));
    for (int id : state.voters) AppendU32Be(&out, static_cast<uint32_t>(id));
    AppendU32Be(&out, static_cast<uint32_t>(state.next.size()));
    for (int id : state.next) AppendU32Be(&out, static_cast<uint32_t>(id));
    AppendU64Be(&out, static_cast<uint64_t>(state.change_index));
    return out;
}

inline bool DecodeMembership(const std::string& raw, MembershipState* state) {
    if (raw.size() < 2 || raw[0] != 1) return false;
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
    if (!ReadU64Be(raw, &offset, &change) || offset != raw.size()) return false;
    if (change > static_cast<uint64_t>(INT64_MAX)) return false;
    parsed.change_index = static_cast<int64_t>(change);
    if (parsed.joint && parsed.change_index <= 0) return false;
    if (!parsed.joint && parsed.change_index != 0) return false;
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
    *ok = false;
    return true;
}

inline std::string MemberCommandText(MemberKind kind, int peer) {
    std::vector<std::string> args = {"MEMBER"};
    if (kind == MemberKind::Commit) args.push_back("COMMIT");
    else {
        args.push_back(kind == MemberKind::Join ? "JOIN" : "LEAVE");
        args.push_back(std::to_string(peer));
    }
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}
