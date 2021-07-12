#ifndef ARKI_STREAM_CONCRETE_H
#define ARKI_STREAM_CONCRETE_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>

namespace arki {
namespace stream {

template<typename Backend>
struct ConcreteStreamOutputBase: public BaseStreamOutput
{
    std::shared_ptr<core::NamedFileDescriptor> out;
    int orig_fl = -1;
    pollfd pollinfo;
    bool has_splice = false;

    /**
     * Returns:
     *  0 if the pipe can accept new data
     *  an OR combination of SendResult flags if a known condition happened
     *  that should interrupt the writing
     *
     * May throw TimedOut, or a std::runtime_error in case of errors
     */
    uint32_t wait_writable();

    stream::SendResult _write_output_buffer(const void* data, size_t size);

    ConcreteStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1);
    ~ConcreteStreamOutputBase();

    std::string name() const override;

    void start_filter(const std::vector<std::string>& command) override;

    template<template<typename> class ToPipe, typename... Args>
    SendResult _send_from_pipe(Args&&... args);

    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    std::pair<size_t, SendResult> send_from_pipe(int fd) override;
};

class ConcreteStreamOutput: public ConcreteStreamOutputBase<ConcreteLinuxBackend>
{
public:
    using ConcreteStreamOutputBase::ConcreteStreamOutputBase;
};


}
}

#endif
