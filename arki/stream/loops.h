#ifndef ARKI_STREAM_LOOPS_H
#define ARKI_STREAM_LOOPS_H

#include <arki/stream/fwd.h>
#include <arki/stream/base.h>
#include <stdexcept>
#include <cerrno>

namespace arki {
namespace stream {

template<typename Backend>
struct ConcreteStreamOutputBase;

enum class TransferResult
{
    DONE = 0,
    EOF_SOURCE = 1,
    EOF_DEST = 2,
    WOULDBLOCK = 3,
};

class SendfileNotAvailable : std::exception
{
    using std::exception::exception;
};

class SpliceNotAvailable : std::exception
{
    using std::exception::exception;
};

static const unsigned POLLINFO_FILTER_STDIN = 0;
static const unsigned POLLINFO_FILTER_STDOUT = 1;
static const unsigned POLLINFO_FILTER_STDERR = 2;
static const unsigned POLLINFO_DESTINATION = 3;

static inline bool errno_wouldblock()
{
    if (errno == EAGAIN) return true;
    if (errno == EWOULDBLOCK) return true;
    return false;
}

/**
 * Event loop used by ConcreteStreamOutputs for sending data to an output file
 * descriptor, without filters
 */
template<typename Backend>
struct UnfilteredLoop : public Sender
{
    ConcreteStreamOutputBase<Backend>& stream;
    pollfd pollinfo;

    UnfilteredLoop(ConcreteStreamOutputBase<Backend>& stream);

    template<typename ToOutput>
    stream::SendResult loop(ToOutput to_output);

    stream::SendResult send_buffer(const void* data, size_t size) override final;
    stream::SendResult send_line(const void* data, size_t size) override final;
    stream::SendResult send_file_segment(core::NamedFileDescriptor& src_fd, off_t offset, size_t size) override final;
    stream::SendResult flush() override final { return stream::SendResult(); }
};

}
}

#endif
