#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

SendResult ConcreteStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    utils::Sigignore ignpipe(SIGPIPE);
    size_t pos = 0;
    while (pos < size)
    {
        ssize_t res = ::write(*out, (const uint8_t*)data + pos, size - pos);
        if (res < 0)
        {
            if (errno == EPIPE)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out->name());
        }
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            break;
        }
        if (progress_callback)
            progress_callback(res);
        pos += res;
        result.sent += res;
    }

    return result;
}

SendResult ConcreteStreamOutput::send_line(const void* data, size_t size)
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
    ssize_t res = ::writev(*out, todo, 2);
    if (res < 0)
    {
        if (errno == EPIPE)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_DEST;
        } else
            throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out->name());
    } else {
        if ((unsigned)res != size + 1)
            throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out->name());
        if (progress_callback)
            progress_callback(res);
        result.sent += res;
    }
    return result;
}

SendResult ConcreteStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
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
    while (size > 0)
    {
        if (has_sendfile)
        {
            utils::Sigignore ignpipe(SIGPIPE);
            ssize_t res = ::sendfile(*out, fd, &offset, size);
            if (res < 0)
            {
                if (errno == EINVAL || errno == ENOSYS)
                {
                    has_sendfile = false;
                    buffer.allocate();
                } else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                }
                else
                    throw std::system_error(errno, std::system_category(), "cannot sendfile " + std::to_string(size + 1) + " bytes from " + fd.name() + " to " + out->name());
            } else if (res == 0) {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            } else {
                if (progress_callback)
                    progress_callback(res);
                size -= res;
                result.sent += res;
            }
        } else {
            size_t res = fd.pread(buffer, std::min(size, buffer.size), offset);
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }
            result += send_buffer(buffer, res);
            offset += res;
            size -= res;
        }
    }

    arki::utils::acct::plain_data_read_count.incr();
    // iotrace::trace_file(dirfd, offset, size, "streamed data");
    return result;
}

SendResult ConcreteStreamOutput::send_from_pipe(int fd)
{
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
        {
            if (errno == EAGAIN) {
                result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                return result;
            } else
                throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        }
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
            if (res > 0)
            {
                if (progress_callback)
                    progress_callback(res);
                result.sent += res;
            } else if (res == 0) {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            } else if (res < 0) {
                if (errno == EINVAL)
                {
                    // Splice is not supported: pass it on to the traditional method
                    has_splice = false;
                    buffer.allocate();
                    continue;
                } else if (errno == EPIPE) {
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    break;
                } else if (errno == EAGAIN) {
                    result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
            }

#else
            // Splice is not supported: pass it on to the traditional method
            has_splice = false;
            buffer.allocate();
#endif
        } else {
            // Fall back to read/write

            // Read data from child
            buffer.allocate();
            ssize_t res = read(fd, buffer, buffer.size);
            if (res < 0)
            {
                if (errno == EAGAIN) {
                    result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                    break;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
            }
            if (res == 0)
            {
                result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                break;
            }

            // Pass it on
            result += send_buffer(buffer, res);
        }
    }

    return result;
}

}
}
