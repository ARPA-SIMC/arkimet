#ifndef ARKI_STREAM_CONCRETE_TIMEOUT_H
#define ARKI_STREAM_CONCRETE_TIMEOUT_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class ConcreteTimeoutStreamOutput: public BaseStreamOutput
{
    core::NamedFileDescriptor& out;
    unsigned timeout_ms;
    int orig_fl = -1;
    pollfd pollinfo;

    void wait_readable();

public:
    ConcreteTimeoutStreamOutput(core::NamedFileDescriptor& out, unsigned timeout_ms);
    ~ConcreteTimeoutStreamOutput();

    size_t send_line(const void* data, size_t size) override;
    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    size_t send_buffer(const void* data, size_t size) override;
    size_t send_from_pipe(int fd) override;
};

}
}

#endif
