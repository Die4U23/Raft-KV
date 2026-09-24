#pragma once
#include "raft/raft_codec.h"
#include <array>
#include <cstdint>
#include <cstring>
#include <string>

// HMAC-SHA256 around a RaftCodec frame. An empty token is a no-op so existing
// frames stay byte-compatible. A non-empty token rejects a missing or bad MAC
// instead of accepting the inner frame.

namespace frame_mac {
inline uint32_t Rotr(uint32_t value, uint32_t bits) {
    return (value >> bits) | (value << (32 - bits));
}

inline std::array<uint8_t, 32> Sha256(const uint8_t* data, size_t length) {
    static const uint32_t k[64] = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
    uint32_t hash[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    const uint64_t bits = static_cast<uint64_t>(length) * 8;
    size_t padded = length + 1;
    while (padded % 64 != 56) ++padded;
    std::string block(padded + 8, '\0');
    if (length != 0) std::memcpy(block.data(), data, length);
    block[length] = static_cast<char>(0x80);
    for (int i = 0; i < 8; ++i)
        block[padded + static_cast<size_t>(i)] =
            static_cast<char>((bits >> (56 - 8 * i)) & 0xff);
    for (size_t offset = 0; offset < block.size(); offset += 64) {
        uint32_t words[64];
        for (int i = 0; i < 16; ++i) {
            const size_t base = offset + static_cast<size_t>(i) * 4;
            words[i] = (uint32_t(uint8_t(block[base])) << 24) |
                       (uint32_t(uint8_t(block[base + 1])) << 16) |
                       (uint32_t(uint8_t(block[base + 2])) << 8) |
                       uint32_t(uint8_t(block[base + 3]));
        }
        for (int i = 16; i < 64; ++i) {
            const uint32_t s0 = Rotr(words[i - 15], 7) ^ Rotr(words[i - 15], 18) ^ (words[i - 15] >> 3);
            const uint32_t s1 = Rotr(words[i - 2], 17) ^ Rotr(words[i - 2], 19) ^ (words[i - 2] >> 10);
            words[i] = words[i - 16] + s0 + words[i - 7] + s1;
        }
        uint32_t a = hash[0], b = hash[1], c = hash[2], d = hash[3];
        uint32_t e = hash[4], f = hash[5], g = hash[6], h = hash[7];
        for (int i = 0; i < 64; ++i) {
            const uint32_t s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
            const uint32_t ch = (e & f) ^ ((~e) & g);
            const uint32_t t1 = h + s1 + ch + k[i] + words[i];
            const uint32_t s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
            const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t t2 = s0 + maj;
            h = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
        }
        hash[0] += a; hash[1] += b; hash[2] += c; hash[3] += d;
        hash[4] += e; hash[5] += f; hash[6] += g; hash[7] += h;
    }
    std::array<uint8_t, 32> out{};
    for (int i = 0; i < 8; ++i) {
        out[static_cast<size_t>(i * 4)] = static_cast<uint8_t>(hash[i] >> 24);
        out[static_cast<size_t>(i * 4 + 1)] = static_cast<uint8_t>(hash[i] >> 16);
        out[static_cast<size_t>(i * 4 + 2)] = static_cast<uint8_t>(hash[i] >> 8);
        out[static_cast<size_t>(i * 4 + 3)] = static_cast<uint8_t>(hash[i]);
    }
    return out;
}

inline std::array<uint8_t, 32> HmacSha256(const std::string& key, const std::string& message) {
    constexpr size_t kBlock = 64;
    std::string trimmed = key;
    if (trimmed.size() > kBlock) {
        const auto hashed = Sha256(reinterpret_cast<const uint8_t*>(trimmed.data()), trimmed.size());
        trimmed.assign(reinterpret_cast<const char*>(hashed.data()), hashed.size());
    }
    trimmed.resize(kBlock, '\0');
    std::string inner_pad(kBlock, '\0'), outer_pad(kBlock, '\0');
    for (size_t i = 0; i < kBlock; ++i) {
        inner_pad[i] = static_cast<char>(static_cast<uint8_t>(trimmed[i]) ^ 0x36);
        outer_pad[i] = static_cast<char>(static_cast<uint8_t>(trimmed[i]) ^ 0x5c);
    }
    const std::string inner = inner_pad + message;
    const auto inner_hash = Sha256(reinterpret_cast<const uint8_t*>(inner.data()), inner.size());
    const std::string outer = outer_pad +
        std::string(reinterpret_cast<const char*>(inner_hash.data()), inner_hash.size());
    return Sha256(reinterpret_cast<const uint8_t*>(outer.data()), outer.size());
}

inline bool MacEqual(const uint8_t* left, const uint8_t* right, size_t length) {
    unsigned diff = 0;
    for (size_t i = 0; i < length; ++i) diff |= unsigned(left[i] ^ right[i]);
    return diff == 0;
}
}

enum class FrameSealStatus { NeedMore, Ok, Reject };

inline std::string SealFrame(const std::string& token, const std::string& frame) {
    if (token.empty()) return frame;
    const auto mac = frame_mac::HmacSha256(token, frame);
    const uint32_t inner = static_cast<uint32_t>(32 + frame.size());
    std::string out("MAC1");
    char length[4];
    RaftCodec::WriteU32(length, inner);
    out.append(length, 4);
    out.append(reinterpret_cast<const char*>(mac.data()), mac.size());
    out.append(frame);
    return out;
}

// Empty token accepts one raw RaftCodec frame. A token requires MAC1, the HMAC,
// and exactly one inner frame. Reject closes the stream; NeedMore waits.
inline FrameSealStatus UnsealFrame(const std::string& token, const char* data, size_t length,
                                   std::string* frame, size_t* consumed) {
    if (token.empty()) {
        DecodedRaftMsg message;
        size_t used = 0;
        try {
            if (!RaftCodec::TryDecode(data, length, &message, &used))
                return FrameSealStatus::NeedMore;
        } catch (const std::invalid_argument&) {
            return FrameSealStatus::Reject;
        }
        frame->assign(data, used);
        *consumed = used;
        return FrameSealStatus::Ok;
    }
    if (length < 8) return FrameSealStatus::NeedMore;
    if (std::memcmp(data, "MAC1", 4) != 0) return FrameSealStatus::Reject;
    const uint32_t inner = RaftCodec::ReadU32(data + 4);
    if (inner < 32 || inner > RaftCodec::kMaxFrameSize + 32) return FrameSealStatus::Reject;
    if (length < 8u + inner) return FrameSealStatus::NeedMore;
    const auto expected = frame_mac::HmacSha256(token, std::string(data + 40, inner - 32));
    if (!frame_mac::MacEqual(expected.data(), reinterpret_cast<const uint8_t*>(data + 8), 32))
        return FrameSealStatus::Reject;
    DecodedRaftMsg message;
    size_t used = 0;
    try {
        if (!RaftCodec::TryDecode(data + 40, inner - 32, &message, &used) || used != inner - 32)
            return FrameSealStatus::Reject;
    } catch (const std::invalid_argument&) {
        return FrameSealStatus::Reject;
    }
    frame->assign(data + 40, inner - 32);
    *consumed = 8u + inner;
    return FrameSealStatus::Ok;
}
