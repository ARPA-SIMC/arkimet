#ifndef ARKI_STREAM_CONCRETE_H
#define ARKI_STREAM_CONCRETE_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>

namespace arki {
namespace stream {

template<typename Backend>
class ConcreteStreamOutputBase: public BaseConcreteStreamOutput
{
public:
    using BaseConcreteStreamOutput::BaseConcreteStreamOutput;

    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_from_pipe(int fd) override;
};

class ConcreteStreamOutput: public ConcreteStreamOutputBase<ConcreteLinuxBackend>
{
public:
    using ConcreteStreamOutputBase::ConcreteStreamOutputBase;
};

}
}

#endif
