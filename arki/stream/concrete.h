#ifndef ARKI_STREAM_CONCRETE_H
#define ARKI_STREAM_CONCRETE_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>

namespace arki {
namespace stream {

class ConcreteStreamOutput: public BaseStreamOutput
{
    core::NamedFileDescriptor& out;

public:
    ConcreteStreamOutput(core::NamedFileDescriptor& out)
        : out(out)
    {
    }

    size_t send_line(const void* data, size_t size) override;
    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    size_t send_buffer(const void* data, size_t size) override;
    size_t send_from_pipe(int fd) override;
};

}
}

#endif

