#include "base.h"
#include "arki/utils/sys.h"
#include <poll.h>
#include <system_error>
#include <unistd.h>
#include <fcntl.h>

namespace arki {
namespace stream {

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
