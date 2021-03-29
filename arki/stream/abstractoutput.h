#ifndef ARKI_STREAM_ABSTRACTOUTPUT_H
#define ARKI_STREAM_ABSTRACTOUTPUT_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class AbstractOutputStreamOutput: public BaseStreamOutput
{
    std::shared_ptr<core::AbstractOutputFile> out;

public:
    AbstractOutputStreamOutput(std::shared_ptr<core::AbstractOutputFile> out);

    std::string name() const override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_line(const void* data, size_t size) override;
};

}
}

#endif
