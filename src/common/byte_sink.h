#pragma once
#include <cstddef>

// Destination for a snapshot image. Implementations keep a bounded buffer.
struct ByteSink {
    virtual ~ByteSink() = default;
    virtual void Write(const char* data, size_t n) = 0;
};

// Forward-only snapshot bytes. Rewind starts another pass over the same image.
struct ByteSource {
    virtual ~ByteSource() = default;
    virtual bool Read(char* data, size_t n) = 0;
    virtual void Rewind() = 0;
};
