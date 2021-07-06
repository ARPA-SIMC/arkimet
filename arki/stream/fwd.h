#ifndef ARKI_STREAM_FWD_H
#define ARKI_STREAM_FWD_H

#include <cstddef>
#include <cstdint>

namespace arki {
struct StreamOutput;

namespace stream {

class TimedOut;

struct SendResult
{
    /**
     * The source file descriptor has been closed from the other end
     */
    static constexpr uint32_t SEND_PIPE_EOF_SOURCE      = 0x01;

    /**
     * The destination file descriptor has been closed from the other end
     */
    static constexpr uint32_t SEND_PIPE_EOF_DEST        = 0x02;

    /**
     * Sending from pipe stopped because the source file descriptor is set to
     * nonblocking mode, and there is currently no data available to read
     */
    static constexpr uint32_t SEND_PIPE_EAGAIN_SOURCE   = 0x04;

    size_t sent = 0;
    uint32_t flags = 0;

    SendResult() = default;
    SendResult(const SendResult&) = default;
    SendResult(SendResult&&) = default;
    SendResult(size_t sent) : sent(sent) {}
    SendResult(size_t sent, uint32_t flags) : sent(sent), flags(flags) {}
    SendResult& operator=(const SendResult&) = default;
    SendResult& operator=(SendResult&&) = default;

    bool operator==(const SendResult& r) const
    {
        return sent == r.sent && flags == r.flags;
    }

    bool operator!=(const SendResult& r) const
    {
        return sent != r.sent || flags != r.flags;
    }

    void operator+=(const SendResult& r)
    {
        sent += r.sent;
        flags |= r.flags;
    }
};

}

}

#endif
