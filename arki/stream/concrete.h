#ifndef ARKI_STREAM_CONCRETE_H
#define ARKI_STREAM_CONCRETE_H

#include <arki/stream/base.h>
#include <arki/stream/loops.h>
#include <arki/core/fwd.h>

namespace arki {
namespace stream {

template<typename Backend>
struct ConcreteStreamOutputBase: public BaseStreamOutput
{
    std::shared_ptr<core::NamedFileDescriptor> out;
    int orig_fl = -1;
    pollfd pollinfo;
    UnfilteredLoop<Backend> unfiltered_loop;

    /**
     * Returns:
     *  0 if the pipe can accept new data
     *  an OR combination of SendResult flags if a known condition happened
     *  that should interrupt the writing
     *
     * May throw TimedOut, or a std::runtime_error in case of errors
     */
    uint32_t wait_writable();

    ConcreteStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1);
    ~ConcreteStreamOutputBase();

    std::string name() const override;

    stream::FilterProcess* start_filter(const std::vector<std::string>& command) override;

    void flush_filter_output() override;

    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
};

class ConcreteStreamOutput: public ConcreteStreamOutputBase<LinuxBackend>
{
public:
    using ConcreteStreamOutputBase::ConcreteStreamOutputBase;
};

extern template class ConcreteStreamOutputBase<LinuxBackend>;
extern template class ConcreteStreamOutputBase<TestingBackend>;

}
}

#endif
