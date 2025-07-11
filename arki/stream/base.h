#ifndef ARKI_STREAM_BASE_H
#define ARKI_STREAM_BASE_H

#include <arki/stream.h>
#include <poll.h>
#include <sys/uio.h>

namespace arki {
namespace stream {

#undef TRACE_STREAMING
#ifdef TRACE_STREAMING
#define trace_streaming(...) fprintf(stderr, __VA_ARGS__)
#else
#define trace_streaming(...)                                                   \
    do                                                                         \
    {                                                                          \
    } while (0)
#endif

struct FilterProcess;

struct Sender
{
    virtual ~Sender();

    virtual stream::SendResult send_buffer(const void* data, size_t size) = 0;
    virtual stream::SendResult send_line(const void* data, size_t size)   = 0;
    virtual stream::SendResult
    send_file_segment(core::NamedFileDescriptor& src_fd, off_t offset,
                      size_t size)     = 0;
    virtual stream::SendResult flush() = 0;
};

struct BaseStreamOutput : public StreamOutput
{
    int timeout_ms = -1;
    std::function<void(size_t)> progress_callback;
    std::unique_ptr<FilterProcess> filter_process;
    std::unique_ptr<Sender> filter_sender;

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

    stream::FilterProcess*
    start_filter(const std::vector<std::string>& command) override;
    std::unique_ptr<stream::FilterProcess> stop_filter() override;
    void abort_filter() override;

    /**
     * Copy output from the filter stdout to the destination, without sending
     * data to the filter stdin.
     *
     * This assumes that the filter's stdin has already been closed
     */
    virtual void flush_filter_output() = 0;
};

/**
 * Linux versions of syscalls to use for concrete implementations.
 */
struct LinuxBackend
{
    static ssize_t (*read)(int fd, void* buf, size_t count);
    static ssize_t (*write)(int fd, const void* buf, size_t count);
    static ssize_t (*writev)(int fd, const struct iovec* iov, int iovcnt);
    static ssize_t (*sendfile)(int out_fd, int in_fd, off_t* offset,
                               size_t count);
    static ssize_t (*splice)(int fd_in, loff_t* off_in, int fd_out,
                             loff_t* off_out, size_t len, unsigned int flags);
    static int (*poll)(struct pollfd* fds, nfds_t nfds, int timeout);
    static ssize_t (*pread)(int fd, void* buf, size_t count, off_t offset);
};

/**
 * Mockable versions of syscalls to use for testing concrete implementations.
 */
struct TestingBackend
{
    static std::function<ssize_t(int fd, void* buf, size_t count)> read;
    static std::function<ssize_t(int fd, const void* buf, size_t count)> write;
    static std::function<ssize_t(int fd, const struct iovec* iov, int iovcnt)>
        writev;
    static std::function<ssize_t(int out_fd, int in_fd, off_t* offset,
                                 size_t count)>
        sendfile;
    static std::function<ssize_t(int fd_in, loff_t* off_in, int fd_out,
                                 loff_t* off_out, size_t len,
                                 unsigned int flags)>
        splice;
    static std::function<int(struct pollfd* fds, nfds_t nfds, int timeout)>
        poll;
    static std::function<ssize_t(int fd, void* buf, size_t count, off_t offset)>
        pread;

    static void reset();
};

} // namespace stream
} // namespace arki

#endif
