#ifndef ARKI_STREAM_BUFFER_H
#define ARKI_STREAM_BUFFER_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class BufferStreamOutput: public BaseStreamOutput
{
    std::vector<uint8_t>& out;

public:
    BufferStreamOutput(std::vector<uint8_t>& out);

    std::string name() const override { return "memory buffer"; }
    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_from_pipe(int fd) override;
};

}
}

#endif
