#ifndef ARKI_STREAM_ABSTRACT_H
#define ARKI_STREAM_ABSTRACT_H

#include <arki/stream/base.h>
#include <arki/core/fwd.h>

namespace arki {
namespace stream {

template<typename Backend>
struct AbstractStreamOutput : public BaseStreamOutput
{
    /**
     * Low-level function to write the given buffer to the output.
     *
     * This does not do filtering and does not trigger data start callbacks: it
     * just writes the data out to the final destination
     */
    virtual stream::SendResult _write_output_buffer(const void* data, size_t size) = 0;

    /**
     * Low-level function to write the given buffer, plus a newline, to the
     * output.
     *
     * This does not do filtering and does not trigger data start callbacks: it
     * just writes the data out to the final destination.
     *
     * By default it is implemented with calls to _write_output_buffer
     */
    virtual stream::SendResult _write_output_line(const void* data, size_t size);

    template<template<typename> class ToPipe, typename... Args>
    SendResult _send_from_pipe(Args&&... args);

    void flush_filter_output() override;

    using BaseStreamOutput::BaseStreamOutput;

    // Generic implementation based on _write_output_buffer
    SendResult send_buffer(const void* data, size_t size) override;

    // Generic implementation based on _write_output_line
    SendResult send_line(const void* data, size_t size) override;

    // Generic implementation based on _write_output_buffer
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
};

extern template class AbstractStreamOutput<LinuxBackend>;
extern template class AbstractStreamOutput<TestingBackend>;

}
}

#endif
