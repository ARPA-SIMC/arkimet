#ifndef ARKI_STREAM_BUFFER_H
#define ARKI_STREAM_BUFFER_H

#include <arki/stream/abstract.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class BufferStreamOutput: public AbstractStreamOutput<LinuxBackend>
{
    std::vector<uint8_t>& out;

    stream::SendResult _write_output_buffer(const void* data, size_t size) override;
    stream::SendResult _write_output_line(const void* data, size_t size) override;

public:
    BufferStreamOutput(std::vector<uint8_t>& out);

    std::string name() const override { return "memory buffer"; }
};

}
}

#endif
