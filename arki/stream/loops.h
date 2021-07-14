#ifndef ARKI_STREAM_LOOPS_H
#define ARKI_STREAM_LOOPS_H

#include <arki/stream/fwd.h>
#include <arki/stream/base.h>
#include <stdexcept>

namespace arki {
namespace stream {

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

}
}

#endif
