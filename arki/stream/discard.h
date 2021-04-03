#ifndef ARKI_STREAM_DISCARD_H
#define ARKI_STREAM_DISCARD_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class DiscardStreamOutput: public BaseStreamOutput
{
public:
    DiscardStreamOutput();

    std::string name() const override { return "discard"; }
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_line(const void* data, size_t size) override;
};

}
}

#endif
