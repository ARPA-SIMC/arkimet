#ifndef ARKI_STREAM_BASE_H
#define ARKI_STREAM_BASE_H

#include <arki/stream.h>
#include <sys/uio.h>
#include <poll.h>

namespace arki {
namespace stream {

struct FilterProcess;

class BaseStreamOutput : public StreamOutput
{
protected:
    std::function<void(size_t)> progress_callback;
    std::function<stream::SendResult(StreamOutput&)> data_start_callback;
    std::unique_ptr<FilterProcess> filter_process;

    /**
     * Fire data_start_callback then set it to nullptr
     *
     * It disarms the callback before firing it, to prevent firing it
     * recursively if it calls other send_ operations on the stream.
     */
    stream::SendResult fire_data_start_callback()
    {
        auto cb = data_start_callback;
        data_start_callback = nullptr;
        return cb(*this);
    }

    /**
     * Returns:
     *  0 if the source has data available
     *  an OR combination of SendResult flags if a known condition happened
     *  that should interrupt the writing
     */
    uint32_t wait_readable(int read_fd);

    /**
     * Check if a file descriptor is in nonblocking mode
     */
    bool is_nonblocking(int fd);

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

public:
    BaseStreamOutput();
    virtual ~BaseStreamOutput();

    void set_progress_callback(std::function<void(size_t)> f) override
    {
        progress_callback = f;
    }

    void set_data_start_callback(std::function<stream::SendResult(StreamOutput&)> f) override
    {
        data_start_callback = f;
    }

    void set_filter_command(const std::vector<std::string>& command) override;
    void unset_filter_command() override;

    // Generic implementation based on _write_output_buffer
    SendResult send_buffer(const void* data, size_t size) override;

    // Generic implementation based on _write_output_line
    SendResult send_line(const void* data, size_t size) override;

    // Generic implementation based on _write_output_buffer
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;

    // Generic implementation based on _write_output_buffer
    SendResult send_from_pipe(int fd) override;
};

struct TransferBuffer
{
    constexpr static size_t size = 4096 * 8;
    char* buf = nullptr;

    TransferBuffer() = default;
    TransferBuffer(const TransferBuffer&) = delete;
    TransferBuffer(TransferBuffer&&) = delete;
    ~TransferBuffer()
    {
        delete[] buf;
    }
    TransferBuffer& operator=(const TransferBuffer&) = delete;
    TransferBuffer& operator=(TransferBuffer&&) = delete;

    void allocate()
    {
        if (buf)
            return;
        buf = new char[size];
    }

    operator char*() { return buf; }
};

}
}

#endif
