#pragma once
#include "common/resp_parser.h"

// Keep unread TCP bytes across receives. Consuming a pipelined command advances
// a cursor instead of repeatedly copying the remaining pipeline.
class CommandBuffer {
public:
    static constexpr size_t kMaxBufferedBytes = 4 * 1024 * 1024;

    bool Append(std::string_view chunk) {
        if (chunk.empty()) return true;
        if (chunk.size() > kMaxBufferedBytes - UnreadBytes()) return false;
        if (offset_ != 0) {
            bytes_.erase(0, offset_);
            offset_ = 0;
        }
        bytes_.append(chunk.data(), chunk.size());
        return true;
    }

    RespParser::Result Next() {
        auto result = RespParser::TryParseOne(std::string_view(bytes_).substr(offset_));
        if (result.state == RespParser::State::Complete) {
            offset_ += result.consumed;
            if (offset_ == bytes_.size()) {
                if (bytes_.capacity() > 64 * 1024) std::string().swap(bytes_);
                else bytes_.clear();
                offset_ = 0;
            } else if (offset_ >= 64 * 1024 && UnreadBytes() <= 64 * 1024) {
                // Reclaim a large consumed prefix even if the client leaves a
                // small incomplete suffix idle. Copies remain amortized linear.
                std::string remaining(bytes_.data() + offset_, UnreadBytes());
                bytes_.swap(remaining);
                offset_ = 0;
            }
        }
        return result;
    }

    size_t UnreadBytes() const { return bytes_.size() - offset_; }

private:
    std::string bytes_;
    size_t offset_ = 0;
};
