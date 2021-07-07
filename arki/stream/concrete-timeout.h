#ifndef ARKI_STREAM_CONCRETE_TIMEOUT_H
#define ARKI_STREAM_CONCRETE_TIMEOUT_H

#include <arki/stream/concrete.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

template<typename Backend>
class ConcreteTimeoutStreamOutputBase: public ConcreteStreamOutputBase<Backend>
{
public:
    using ConcreteStreamOutputBase<Backend>::ConcreteStreamOutputBase;

    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_from_pipe(int fd) override;
};

class ConcreteTimeoutStreamOutput: public ConcreteTimeoutStreamOutputBase<ConcreteLinuxBackend>
{
    using ConcreteTimeoutStreamOutputBase::ConcreteTimeoutStreamOutputBase;
};

}
}

#endif
