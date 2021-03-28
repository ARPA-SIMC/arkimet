#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

size_t ConcreteStreamOutput::send_buffer(const void* data, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    if (data_start_callback)
        sent += fire_data_start_callback();

    size_t pos = 0;
    while (pos < size)
    {
        ssize_t res = ::write(out, (const uint8_t*)data + pos, size - pos);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to stream");
        if (res == 0)
            break;
        if (progress_callback)
            progress_callback(res);
        pos += res;
        sent += res;
    }

    return sent;

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

size_t ConcreteStreamOutput::send_line(const void* data, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    if (data_start_callback) sent += fire_data_start_callback();

    struct iovec todo[2] = {
        { (void*)data, size },
        { (void*)"\n", 1 },
    };
    ssize_t res = ::writev(out, todo, 2);
    if (res < 0 || (unsigned)res != size + 1)
        throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
    if (progress_callback)
        progress_callback(res);
    sent += size + 1;
    return sent;
}

size_t ConcreteStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    TransferBuffer buffer;
    if (data_start_callback)
    {
        // If we have data_start_callback, we need to do a regular read/write
        // cycle before attempting the handover to sendfile, to see if there
        // actually are data to read and thus output to generate.
        buffer.allocate();
        size_t res = fd.pread(buffer, std::min(size, buffer.size), offset);
        if (res == 0)
            return sent;

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the sendfile handover attempt
        sent += send_buffer(buffer, res);

        offset += res;
        size -= res;
    }

    bool has_sendfile = true;
    while (size > 0)
    {
        if (has_sendfile)
        {
            ssize_t res = ::sendfile(out, fd, &offset, size);
            if (res < 0)
            {
                if (errno == EINVAL || errno == ENOSYS)
                {
                    has_sendfile = false;
                    buffer.allocate();
                }
                else
                    throw std::system_error(errno, std::system_category(), "cannot sendfile " + std::to_string(size + 1) + " bytes from " + fd.name() + " to " + out.name());
            } else if (res == 0)
                break;
            else {
                if (progress_callback)
                    progress_callback(res);
                size -= res;
                sent += res;
            }
        } else {
            size_t res = fd.pread(buffer, std::min(size, buffer.size), offset);
            if (res == 0)
                break;
            res = send_buffer(buffer, res);
            sent += res;
            offset += res;
            size -= res;
        }
    }

    arki::utils::acct::plain_data_read_count.incr();
    // iotrace::trace_file(dirfd, offset, size, "streamed data");
    return sent;
}

size_t ConcreteStreamOutput::send_from_pipe(int fd)
{
    size_t sent = 0;

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
            return sent;

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the splice handover attempt
        sent += send_buffer(buffer, res);
    }

    ssize_t res;
#if defined(__linux__) && defined(HAVE_SPLICE)
    // Try splice
    res = splice(fd, NULL, out, NULL, 4096 * 8, SPLICE_F_MORE);
    if (res >= 0)
    {
        if (progress_callback)
            progress_callback(res);
        sent += res;
        return sent;
    }

    if (errno != EINVAL)
        throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");

    // Splice is not supported: pass it on to the traditional method
#endif
    // Fall back to read/write

    // Read data from child
    buffer.allocate();
    res = read(fd, buffer, buffer.size);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
    if (res == 0)
        return sent;

    // Pass it on
    sent += send_buffer(buffer, res);
    return sent;
}

}
}

