#include "base.h"
#include "arki/utils/sys.h"
#include <poll.h>
#include <system_error>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

ssize_t (*ConcreteLinuxBackend::read)(int fd, void *buf, size_t count) = ::read;
ssize_t (*ConcreteLinuxBackend::write)(int fd, const void *buf, size_t count) = ::write;
ssize_t (*ConcreteLinuxBackend::writev)(int fd, const struct iovec *iov, int iovcnt) = ::writev;
ssize_t (*ConcreteLinuxBackend::sendfile)(int out_fd, int in_fd, off_t *offset, size_t count) = ::sendfile;
ssize_t (*ConcreteLinuxBackend::splice)(int fd_in, loff_t *off_in, int fd_out,
                                                    loff_t *off_out, size_t len, unsigned int flags) = ::splice;
int (*ConcreteLinuxBackend::poll)(struct pollfd *fds, nfds_t nfds, int timeout) = ::poll;
ssize_t (*ConcreteLinuxBackend::pread)(int fd, void *buf, size_t count, off_t offset) = ::pread;

std::function<ssize_t(int fd, void *buf, size_t count)> ConcreteTestingBackend::read = ::read;
std::function<ssize_t(int fd, const void *buf, size_t count)> ConcreteTestingBackend::write = ::write;
std::function<ssize_t(int fd, const struct iovec *iov, int iovcnt)> ConcreteTestingBackend::writev = ::writev;
std::function<ssize_t(int out_fd, int in_fd, off_t *offset, size_t count)> ConcreteTestingBackend::sendfile = ::sendfile;
std::function<ssize_t(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags)> ConcreteTestingBackend::splice = ::splice;
std::function<int(struct pollfd *fds, nfds_t nfds, int timeout)> ConcreteTestingBackend::poll = ::poll;
std::function<ssize_t(int fd, void *buf, size_t count, off_t offset)> ConcreteTestingBackend::pread = ::pread;


size_t constexpr TransferBuffer::size;

uint32_t BaseStreamOutput::wait_readable(int read_fd)
{
    pollfd pollinfo;
    pollinfo.fd = read_fd;
    pollinfo.events = POLLIN | POLLRDHUP;
    pollinfo.revents = 0;

    // Wait for available input data
    int res = ::poll(&pollinfo, 1, 0);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on input pipe");
    if (res == 0)
        return SendResult::SEND_PIPE_EAGAIN_SOURCE;
    if (pollinfo.revents & POLLRDHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo.revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo.revents & POLLHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (! (pollinfo.revents & POLLIN))
        throw std::runtime_error("unsupported revents values when polling input pipe");

    return 0;
}

bool BaseStreamOutput::is_nonblocking(int fd)
{
    int src_fl = fcntl(fd, F_GETFL);
    if (src_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for input");
    return src_fl & O_NONBLOCK;
}

SendResult BaseStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    TransferBuffer buffer;
    buffer.allocate();

    size_t pos = 0;
    while (pos < size)
    {
        size_t res = fd.pread(buffer, std::min(buffer.size, size - pos), offset + pos);
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            break;
        }
        result += send_buffer(buffer, res);
        pos += res;
    }

    return result;
}

SendResult BaseStreamOutput::send_from_pipe(int fd)
{
    SendResult result;

    TransferBuffer buffer;
    buffer.allocate();
    while (true)
    {
        ssize_t res = read(fd, buffer, buffer.size);
        if (res < 0)
        {
            if (errno == EAGAIN) {
                result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot read from pipe");
        }
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            break;
        }

        result += send_buffer(buffer, res);
    }

    return result;
}


std::string BaseConcreteStreamOutput::name() const { return out->name(); }

}
}
