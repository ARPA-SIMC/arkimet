#include "concrete-timeout.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

uint32_t ConcreteTimeoutStreamOutput::wait_writable()
{
    pollinfo[0].revents = 0;
    int res = ::poll(pollinfo, 1, timeout_ms);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on " + out.name());
    if (res == 0)
        throw TimedOut("write on " + out.name() + " timed out");
    if (pollinfo[0].revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_DEST;
    if (pollinfo[0].revents & POLLOUT)
        return 0;
    throw std::runtime_error("unsupported revents values when polling " + out.name());
}

uint32_t ConcreteTimeoutStreamOutput::wait_readable_writable(int read_fd)
{
    pollinfo[0].revents = 0;
    pollinfo[1].fd = read_fd;
    pollinfo[1].revents = 0;

    // Wait for available input data
    int res = ::poll(&pollinfo[1], 1, 0);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on input pipe");
    if (res == 0)
        return SendResult::SEND_PIPE_EAGAIN_SOURCE;
    if (pollinfo[1].revents & POLLRDHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo[1].revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo[1].revents & POLLHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (! (pollinfo[1].revents & POLLIN))
        throw std::runtime_error("unsupported revents values when polling input pipe");

    // Wait for output pipe to be writable
    res = ::poll(pollinfo, 1, timeout_ms);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on " + out.name());
    if (res == 0)
        throw TimedOut("write on " + out.name() + " timed out");
    if (pollinfo[0].revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_DEST;
    if (!(pollinfo[0].revents & POLLOUT))
        throw std::runtime_error("unsupported revents values when polling " + out.name());

    return 0;
}

ConcreteTimeoutStreamOutput::ConcreteTimeoutStreamOutput(core::NamedFileDescriptor& out, unsigned timeout_ms)
    : out(out), timeout_ms(timeout_ms)
{
    orig_fl = fcntl(out, F_GETFL);
    if (orig_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for " + out.name());
    if (fcntl(out, F_SETFL, orig_fl | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for " + out.name());

    pollinfo[0].fd = out;
    pollinfo[0].events = POLLOUT;
    pollinfo[1].events = POLLIN | POLLRDHUP;
}

ConcreteTimeoutStreamOutput::~ConcreteTimeoutStreamOutput()
{
    // If out is still open, reset as it was before
    if (out != -1)
        fcntl(out, F_SETFL, orig_fl);
}

SendResult ConcreteTimeoutStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    utils::Sigignore ignpipe(SIGPIPE);
    size_t pos = 0;
    while (true)
    {
        ssize_t res = ::write(out, (const uint8_t*)data + pos, size - pos);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                res = 0;
            else if (errno == EPIPE) {
                result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out.name());
        }

        pos += res;
        result.sent += res;
        if (progress_callback)
            progress_callback(res);

        if (pos >= size)
            break;

        uint32_t wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }
    return result;

#if 0
    Sigignore ignpipe(SIGPIPE);
    size_t pos = 0;
    while (m_nextfd && pos < (size_t)res)
    {
        ssize_t wres = write(*m_nextfd, buf+pos, res-pos);
        if (wres < 0)
        {
            if (errno == EPIPE)
            {
                m_nextfd = nullptr;
            } else
                throw_system_error("writing to destination file descriptor");
        }
        pos += wres;
    }
#endif
}

SendResult ConcreteTimeoutStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    utils::Sigignore ignpipe(SIGPIPE);
    struct iovec todo[2] = {
        { (void*)data, size },
        { (void*)"\n", 1 },
    };
    size_t pos = 0;
    while (true)
    {
        if (pos < size)
        {
            ssize_t res = ::writev(out, todo, 2);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
            }
            pos += res;
            result.sent += res;
            if (pos < size + 1)
            {
                todo[0].iov_base = (uint8_t*)data + pos;
                todo[0].iov_len = size - pos;
            }
            if (progress_callback)
                progress_callback(res);
        } else if (pos == size) {
            ssize_t res = ::write(out, "\n", 1);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot write 1 byte to " + out.name());
            }
            pos += res;
            result.sent += res;
            if (progress_callback)
                progress_callback(res);
        } else
            break;

        uint32_t wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }
    return result;
}

SendResult ConcreteTimeoutStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    TransferBuffer buffer;
    if (data_start_callback)
    {
        // If we have data_start_callback, we need to do a regular read/write
        // cycle before attempting the handover to sendfile, to see if there
        // actually are data to read and thus output to generate.
        buffer.allocate();
        size_t res = fd.pread(buffer, std::min(size, buffer.size), offset);
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            return result;
        }

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the sendfile handover attempt
        result += send_buffer(buffer, res);

        offset += res;
        size -= res;
    }

    bool has_sendfile = true;
    size_t written = 0;
    while (size > 0)
    {
        if (has_sendfile)
        {
            utils::Sigignore ignpipe(SIGPIPE);
            ssize_t res = ::sendfile(out, fd, &offset, size - written);
            if (res < 0)
            {
                if (errno == EINVAL || errno == ENOSYS)
                {
                    has_sendfile = false;
                    buffer.allocate();
                } else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    res = 0;
                    if (progress_callback)
                        progress_callback(res);
                }
                else
                    throw std::system_error(errno, std::system_category(), "cannot sendfile() " + std::to_string(size) + " bytes to " + out.name());
            } else if (res == 0) {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            } else {
                if (progress_callback)
                    progress_callback(res);
                written += res;
                result.sent += res;
            }
        } else {
            size_t res = out.pread(buffer, std::min(size - written, buffer.size), offset);
            result += send_buffer(buffer, res);
            offset += res;
            written += res;
        }

        utils::acct::plain_data_read_count.incr();

        if (written >= size)
            break;

        // iotrace::trace_file(dirfd, offset, size, "streamed data");

        uint32_t wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }

    return result;
}

SendResult ConcreteTimeoutStreamOutput::send_from_pipe(int fd)
{
    int src_fl = fcntl(fd, F_GETFL);
    if (src_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for input");
    bool src_nonblock = src_fl & O_NONBLOCK;

    SendResult result;

    TransferBuffer buffer;
    if (data_start_callback)
    {
        // If we have data_start_callback, we need to do a regular read/write
        // cycle before attempting the handover to splice, to see if there
        // actually are data to read and thus output to generate.
        buffer.allocate();
        ssize_t res = ::read(fd, buffer, buffer.size);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            return result;
        }

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the splice handover attempt
        result += send_buffer(buffer, res);
    }

    bool has_splice = true;
    while (true)
    {
        if (has_splice)
        {
#ifdef HAVE_SPLICE
            utils::Sigignore ignpipe(SIGPIPE);
            // Try splice
            ssize_t res = splice(fd, NULL, out, NULL, TransferBuffer::size * 128, SPLICE_F_MORE);
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
            else if (res < 0)
            {
                if (errno == EINVAL)
                {
                    has_splice = false;
                    buffer.allocate();
                    continue;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    res = 0;
                    // In theory we don't need to call this. In practice, it
                    // helps unit tests to be able to hook here to empty the
                    // output pipe
                    if (progress_callback)
                        progress_callback(res);
                } else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
            } else if (res > 0) {
                result.sent += res;
                if (progress_callback)
                    progress_callback(res);
            }

#else
            // Splice is not supported: pass it on to the traditional method
            has_splice = false;
            buffer.allocate();
#endif
        } else {
            // Fall back to read/write
            ssize_t res = ::read(fd, buffer, buffer.size);
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else
                    throw std::system_error(errno, std::system_category(), "cannot read data from pipe input");
            }
            if (res > 0)
                result += send_buffer(buffer, res);
            else
            {
                if (progress_callback)
                    progress_callback(res);
            }
        }

        uint32_t wres;
        if (src_nonblock)
            wres = wait_readable_writable(fd);
        else
            wres = wait_writable();
        if (wres)
        {
            result.flags |= wres;
            break;
        }
    }

    return result;
}

}
}
