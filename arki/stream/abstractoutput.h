#ifndef ARKI_STREAM_ABSTRACTOUTPUT_H
#define ARKI_STREAM_ABSTRACTOUTPUT_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class AbstractOutputStreamOutput: public BaseStreamOutput
{
    core::AbstractOutputFile& out;

public:
    AbstractOutputStreamOutput(core::AbstractOutputFile& out);

    size_t send_line(const void* data, size_t size) override;
    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    size_t send_buffer(const void* data, size_t size) override;
    size_t send_from_pipe(int fd) override;
};

}
}

#endif
