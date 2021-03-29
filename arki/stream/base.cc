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


std::string BaseConcreteStreamOutput::name() const { return out->name(); }

}
}
