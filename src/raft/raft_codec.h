#pragma once
#include <string>
#include <cstdint>
#include <cstring>
#include <stdexcept>

// Raft RPC 消息的二进制帧编解码
// 帧格式:
//   [4 bytes BE payload_len] [1 byte msg_type] [4 bytes BE sender_id] [N bytes protobuf]
//   payload_len = 5 + N (即 msg_type + sender_id + protobuf 的总长度)
//
// 不使用 protobuf oneof wrapper，而是用简单的二进制头来复用多种 RPC 类型

enum class RaftMsgType : uint8_t {
    kRequestVote          = 0,
    kRequestVoteResponse  = 1,
    kAppendEntries        = 2,
    kAppendEntriesResponse = 3,
};

// 解码后的消息结构
struct DecodedRaftMsg {
    RaftMsgType type;
    int32_t     sender_id;
    std::string payload;  // 原始 protobuf 字节
};

class RaftCodec {
public:
    static constexpr size_t kHeaderSize = 9;   // 4(len) + 1(type) + 4(sender)
    static constexpr size_t kMaxFrameSize = 10 * 1024 * 1024; // 10 MB

    // 编码: 返回完整的帧字节（含 4 字节长度前缀）
    static std::string Encode(RaftMsgType type, int32_t sender_id,
                              const std::string& protobuf_payload) {
        uint32_t payload_len = 5 + static_cast<uint32_t>(protobuf_payload.size());
        std::string frame(4 + payload_len, '\0');

        // 4 字节大端序 payload_len
        frame[0] = static_cast<char>((payload_len >> 24) & 0xFF);
        frame[1] = static_cast<char>((payload_len >> 16) & 0xFF);
        frame[2] = static_cast<char>((payload_len >> 8) & 0xFF);
        frame[3] = static_cast<char>(payload_len & 0xFF);

        // 1 字节 msg_type
        frame[4] = static_cast<char>(static_cast<uint8_t>(type));

        // 4 字节大端序 sender_id
        uint32_t sid = static_cast<uint32_t>(sender_id);
        frame[5] = static_cast<char>((sid >> 24) & 0xFF);
        frame[6] = static_cast<char>((sid >> 16) & 0xFF);
        frame[7] = static_cast<char>((sid >> 8) & 0xFF);
        frame[8] = static_cast<char>(sid & 0xFF);

        // protobuf payload
        std::memcpy(&frame[9], protobuf_payload.data(), protobuf_payload.size());

        return frame;
    }

    // 尝试从 buffer 中解码一个帧
    // data: 指向 buffer 数据的指针
    // len: buffer 中可读的字节数
    // msg: 输出解码后的消息
    // consumed: 输出消耗的字节数（包括帧头+payload）
    // 返回: true=成功解码, false=数据不完整（需要更多字节）
    static bool TryDecode(const char* data, size_t len,
                          DecodedRaftMsg* msg, size_t* consumed) {
        if (len < 4) return false;

        // 读取 4 字节大端序 payload_len
        uint32_t payload_len =
            (static_cast<uint8_t>(data[0]) << 24) |
            (static_cast<uint8_t>(data[1]) << 16) |
            (static_cast<uint8_t>(data[2]) << 8)  |
            static_cast<uint8_t>(data[3]);

        if (payload_len > kMaxFrameSize) {
            throw std::runtime_error("Raft frame too large: " + std::to_string(payload_len));
        }

        size_t total = 4 + static_cast<size_t>(payload_len);
        if (len < total) return false;

        // 读取 msg_type
        uint8_t type_byte = static_cast<uint8_t>(data[4]);
        msg->type = static_cast<RaftMsgType>(type_byte);

        // 读取 sender_id
        msg->sender_id =
            (static_cast<uint8_t>(data[5]) << 24) |
            (static_cast<uint8_t>(data[6]) << 16) |
            (static_cast<uint8_t>(data[7]) << 8)  |
            static_cast<uint8_t>(data[8]);

        // 读取 protobuf payload
        uint32_t pb_len = payload_len - 5;
        if (pb_len > 0) {
            msg->payload.assign(data + 9, pb_len);
        } else {
            msg->payload.clear();
        }

        *consumed = total;
        return true;
    }
};
