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

    /**
     * Returns:
     *  0 if the pipe can accept new data
     *  an OR combination of SendResult flags if a known condition happened
     *  that should interrupt the writing
     *
     * May throw TimedOut, or a std::runtime_error in case of errors
     */
    uint32_t wait_writable();

public:
    ConcreteTimeoutStreamOutput(core::NamedFileDescriptor& out, unsigned timeout_ms);
    ~ConcreteTimeoutStreamOutput();

    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_from_pipe(int fd) override;
};

}
}

#endif
