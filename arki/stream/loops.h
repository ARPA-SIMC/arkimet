#ifndef ARKI_STREAM_LOOPS_H
#define ARKI_STREAM_LOOPS_H

#include <arki/stream/fwd.h>
#include <arki/stream/base.h>
#include <stdexcept>

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

/**
 * Base class for event loops that implement the streaming operation
 */
struct Sender
{
    BaseStreamOutput& stream;
    stream::SendResult result;

    Sender(BaseStreamOutput& stream)
        : stream(stream)
    {
    }
};


static const unsigned POLLINFO_FILTER_STDIN = 0;
static const unsigned POLLINFO_FILTER_STDOUT = 1;
static const unsigned POLLINFO_FILTER_STDERR = 2;
static const unsigned POLLINFO_DESTINATION = 3;

/**
 * Event loop used by ConcreteStreamOutputs for sending data to an output file
 * descriptor, without filters
 */
template<typename Backend>
struct SenderDirect
{
    ConcreteStreamOutputBase<Backend>& stream;
    pollfd pollinfo;

    SenderDirect(ConcreteStreamOutputBase<Backend>& stream)
        : stream(stream)
    {
        pollinfo.fd = *stream.out;
        pollinfo.events = POLLOUT;
    }

    template<typename ToOutput>
    stream::SendResult loop(ToOutput& to_output);
};

}
}

#endif
