#pragma once
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

enum class RaftMsgType : uint8_t {
    kRequestVote = 0, kRequestVoteResponse = 1,
    kAppendEntries = 2, kAppendEntriesResponse = 3
};
struct DecodedRaftMsg {
    RaftMsgType type{};
    int32_t sender_id = -1;
    std::string payload;
};

class RaftCodec {
public:
    static constexpr size_t kHeaderSize = 9;
    static constexpr size_t kMaxFrameSize = 10 * 1024 * 1024;
    static bool IsValidType(uint8_t type) {
        return type <= static_cast<uint8_t>(RaftMsgType::kAppendEntriesResponse);
    }
    static uint32_t ReadU32(const char* data) {
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i)
            value = (value << 8) | static_cast<uint8_t>(data[i]);
        return value;
    }
    static void WriteU32(char* data, uint32_t value) {
        for (int i = 3; i >= 0; --i) {
            data[i] = static_cast<char>(value & 0xff);
            value >>= 8;
        }
    }
    static std::string Encode(RaftMsgType type, int32_t sender,
                              const std::string& payload) {
        if (sender < 0 || !IsValidType(static_cast<uint8_t>(type)))
            throw std::invalid_argument("invalid Raft frame header");
        if (payload.size() > kMaxFrameSize - 5)
            throw std::length_error("Raft frame too large");
        std::string frame(kHeaderSize + payload.size(), '\0');
        WriteU32(frame.data(), static_cast<uint32_t>(payload.size() + 5));
        frame[4] = static_cast<char>(type);
        WriteU32(frame.data() + 5, static_cast<uint32_t>(sender));
        frame.replace(kHeaderSize, payload.size(), payload);
        return frame;
    }
    // false means incomplete; malformed input throws and must close the stream.
    static bool TryDecode(const char* data, size_t length,
                          DecodedRaftMsg* message, size_t* consumed) {
        *consumed = 0;
        if (length < 4) return false;
        const uint32_t payload_length = ReadU32(data);
        if (payload_length < 5 || payload_length > kMaxFrameSize)
            throw std::invalid_argument("invalid Raft frame length");
        if (length < 4 + static_cast<size_t>(payload_length)) return false;
        const uint8_t type = static_cast<uint8_t>(data[4]);
        const uint32_t sender = ReadU32(data + 5);
        if (!IsValidType(type) || sender > static_cast<uint32_t>(INT32_MAX))
            throw std::invalid_argument("invalid Raft frame header");
        message->type = static_cast<RaftMsgType>(type);
        message->sender_id = static_cast<int32_t>(sender);
        message->payload.assign(data + kHeaderSize, payload_length - 5);
        *consumed = 4 + payload_length;
        return true;
    }
};
