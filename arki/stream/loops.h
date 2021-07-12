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

}
}

#endif
