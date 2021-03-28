#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>

namespace arki {
namespace stream {

size_t ConcreteStreamOutput::send_line(const void* data, size_t size)
{
    struct iovec todo[2] = {
        { (void*)data, size },
        { (void*)"\n", 1 },
    };
    ssize_t res = ::writev(out, todo, 2);
    if (res < 0 || (unsigned)res != size + 1)
        throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
    if (progress_callback)
        progress_callback(res);
    return size + 1;
}

size_t ConcreteStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    fd.sendfile(out, offset, size);
    arki::utils::acct::plain_data_read_count.incr();
    // iotrace::trace_file(dirfd, offset, size, "streamed data");
    if (progress_callback)
        progress_callback(size);
    return size;
}

size_t ConcreteStreamOutput::send_buffer(const void* data, size_t size)
{
    // TODO: retry instead of throw
    out.write_all_or_throw(data, size);
    if (progress_callback)
        progress_callback(size);
    return size;

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

size_t ConcreteStreamOutput::send_from_pipe(int fd)
{
    ssize_t res;
#if defined(__linux__) && defined(HAVE_SPLICE)
    // Try splice
    res = splice(fd, NULL, out, NULL, 4096 * 8, SPLICE_F_MORE);
    if (res >= 0)
    {
        if (progress_callback)
            progress_callback(res);
        return res;
    }

    if (errno != EINVAL)
        throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");

    // Splice is not supported: pass it on to the traditional method
#endif
    // Fall back to read/write

    // Read data from child
    char buf[4096 * 8];
    res = read(fd, buf, 4096 * 8);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
    if (res == 0)
        return 0;

    // Pass it on
    return send_buffer(buf, res);
}

}
}

