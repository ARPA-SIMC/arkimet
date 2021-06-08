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

template<typename Backend>
uint32_t ConcreteTimeoutStreamOutputBase<Backend>::wait_writable()
{
    pollinfo.revents = 0;
    int res = Backend::poll(&pollinfo, 1, timeout_ms);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on " + out->name());
    if (res == 0)
        throw TimedOut("write on " + out->name() + " timed out");
    if (pollinfo.revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_DEST;
    if (pollinfo.revents & POLLOUT)
        return 0;
    throw std::runtime_error("unsupported revents values when polling " + out->name());
}

template<typename Backend>
ConcreteTimeoutStreamOutputBase<Backend>::ConcreteTimeoutStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, unsigned timeout_ms)
    : BaseConcreteStreamOutput(out), timeout_ms(timeout_ms)
{
    orig_fl = fcntl(*out, F_GETFL);
    if (orig_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for " + out->name());
    if (fcntl(*out, F_SETFL, orig_fl | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for " + out->name());

    pollinfo.fd = *out;
    pollinfo.events = POLLOUT;
}

template<typename Backend>
ConcreteTimeoutStreamOutputBase<Backend>::~ConcreteTimeoutStreamOutputBase()
{
    // If out is still open, reset as it was before
    if (*out != -1)
        fcntl(*out, F_SETFL, orig_fl);
}

template<typename Backend>
SendResult ConcreteTimeoutStreamOutputBase<Backend>::send_buffer(const void* data, size_t size)
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
        ssize_t res = Backend::write(*out, (const uint8_t*)data + pos, size - pos);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                res = 0;
            else if (errno == EPIPE) {
                result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out->name());
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

template<typename Backend>
SendResult ConcreteTimeoutStreamOutputBase<Backend>::send_line(const void* data, size_t size)
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
            ssize_t res = Backend::writev(*out, todo, 2);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out->name());
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
            ssize_t res = Backend::write(*out, "\n", 1);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot write 1 byte to " + out->name());
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

template<typename Backend>
SendResult ConcreteTimeoutStreamOutputBase<Backend>::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
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
// Set to 1 to simulate a system without sendfile
#if 0
            has_sendfile = false;
            buffer.allocate();
            continue;
#else
            utils::Sigignore ignpipe(SIGPIPE);
            ssize_t res = Backend::sendfile(*out, fd, &offset, size - written);
            if (res < 0)
            {
                if (errno == EINVAL || errno == ENOSYS)
                {
                    has_sendfile = false;
                    buffer.allocate();
                    continue;
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
                    throw std::system_error(errno, std::system_category(), "cannot sendfile() " + std::to_string(size) + " bytes to " + out->name());
            } else if (res == 0) {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            } else {
                if (progress_callback)
                    progress_callback(res);
                written += res;
                result.sent += res;
            }
#endif
        } else {
            size_t res = fd.pread(buffer, std::min(size - written, buffer.size), offset);
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
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

template<typename Backend>
SendResult ConcreteTimeoutStreamOutputBase<Backend>::send_from_pipe(int fd)
{
    bool src_nonblock = is_nonblocking(fd);

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
            ssize_t res = splice(fd, NULL, *out, NULL, TransferBuffer::size * 128, SPLICE_F_MORE);
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
            // Skip waiting for available I/O and just retry the while
            continue;
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
                // Call progress callback here because we're not calling
                // send_buffer. Send_buffer will take care of calling
                // progress_callback if needed.
                if (progress_callback)
                    progress_callback(res);
            }
        }

        uint32_t wres = 0;
        if (src_nonblock)
            wres = wait_readable(fd);
        if (!wres)
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

