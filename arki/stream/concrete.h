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
    int timeout_ms = -1;
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
    stream::SendResult _write_output_line(const void* data, size_t size);

    ConcreteStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1);
    ~ConcreteStreamOutputBase();

    std::string name() const override;

    void start_filter(const std::vector<std::string>& command) override;
    SendResult send_buffer(const void* data, size_t size) override;
    SendResult send_line(const void* data, size_t size) override;
    SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override;
    std::pair<size_t, SendResult> send_from_pipe(int fd) override;
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


class ConcreteStreamOutput: public ConcreteStreamOutputBase<ConcreteLinuxBackend>
{
public:
    using ConcreteStreamOutputBase::ConcreteStreamOutputBase;
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
