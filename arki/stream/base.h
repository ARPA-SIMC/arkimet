#ifndef ARKI_STREAM_BASE_H
#define ARKI_STREAM_BASE_H

#include <arki/stream.h>
#include <sys/uio.h>
#include <poll.h>

namespace arki {
namespace stream {

struct FilterProcess;

struct BaseStreamOutput : public StreamOutput
{
    int timeout_ms = -1;
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

    void start_filter(const std::vector<std::string>& command) override;
    std::pair<size_t, size_t> stop_filter() override;
    void abort_filter() override;

    virtual void flush_filter_output() = 0;
};


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


struct TransferBuffer
{
    constexpr static size_t size = 4096 * 8;
    char* buf = nullptr;

    TransferBuffer() = default;
    TransferBuffer(const TransferBuffer&) = delete;
    TransferBuffer(TransferBuffer&& o)
        : buf(o.buf)
    {
        o.buf = nullptr;
    }
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


/**
 * Linux versions of syscalls to use for concrete implementations.
 */
struct ConcreteLinuxBackend
{
    static ssize_t (*read)(int fd, void *buf, size_t count);
    static ssize_t (*write)(int fd, const void *buf, size_t count);
    static ssize_t (*writev)(int fd, const struct iovec *iov, int iovcnt);
    static ssize_t (*sendfile)(int out_fd, int in_fd, off_t *offset, size_t count);
    static ssize_t (*splice)(int fd_in, loff_t *off_in, int fd_out,
                             loff_t *off_out, size_t len, unsigned int flags);
    static int (*poll)(struct pollfd *fds, nfds_t nfds, int timeout);
    static ssize_t (*pread)(int fd, void *buf, size_t count, off_t offset);
};


/**
 * Mockable versions of syscalls to use for testing concrete implementations.
 */
struct ConcreteTestingBackend
{
    static std::function<ssize_t(int fd, void *buf, size_t count)> read;
    static std::function<ssize_t(int fd, const void *buf, size_t count)> write;
    static std::function<ssize_t(int fd, const struct iovec *iov, int iovcnt)> writev;
    static std::function<ssize_t(int out_fd, int in_fd, off_t *offset, size_t count)> sendfile;
    static std::function<ssize_t(int fd_in, loff_t *off_in, int fd_out,
                                 loff_t *off_out, size_t len, unsigned int flags)> splice;
    static std::function<int(struct pollfd *fds, nfds_t nfds, int timeout)> poll;
    static std::function<ssize_t(int fd, void *buf, size_t count, off_t offset)> pread;

    static void reset();
};

}
}

#endif
