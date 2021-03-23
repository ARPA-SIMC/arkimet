#include "stream.h"
#include "file.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include <system_error>
#include <cerrno>
#include <sys/uio.h>

using namespace arki::utils;

namespace arki {
namespace core {

StreamOutput::~StreamOutput()
{
}

class ConcreteStreamOutput: public StreamOutput
{
    sys::NamedFileDescriptor& out;

public:
    ConcreteStreamOutput(sys::NamedFileDescriptor& out)
        : out(out)
    {
    }

    size_t send_line(const void* data, size_t size) override
    {
        struct iovec todo[2] = {
            { (void*)data, size },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != size + 1)
            throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
        return size + 1;
    }

    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override
    {
        fd.sendfile(out, offset, size);
        acct::plain_data_read_count.incr();
        // iotrace::trace_file(dirfd, offset, size, "streamed data");
        return size;
    }

    size_t send_buffer(const void* data, size_t size) override
    {
        // TODO: retry instead of throw
        out.write_all_or_throw(data, size);
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

    size_t send_from_pipe(int fd) override
    {
#if defined(__linux__)
#ifdef HAVE_SPLICE
        // Try splice
        ssize_t res = splice(subproc.get_stdout(), NULL, *m_nextfd, NULL, 4096*2, SPLICE_F_MORE);
        if (res >= 0)
            return res;

        if (errno != EINVAL)
            throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");

        // Splice is not supported: pass it on to the traditional method
#endif
#endif
        // Fall back to read/write

        // Read data from child
        char buf[4096 * 8];
        ssize_t res = read(fd, buf, 4096 * 8);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        if (res == 0)
            return 0;

        // Pass it on
        return send_buffer(buf, res);
    }
};


class ConcreteTimeoutStreamOutput: public StreamOutput
{
    sys::NamedFileDescriptor& out;

public:
    ConcreteTimeoutStreamOutput(sys::NamedFileDescriptor& out, unsigned timeout_ms)
        : out(out)
    {
        // TODO: set nonblocking
    }
    ~ConcreteTimeoutStreamOutput()
    {
        // TODO: if still open, reset as it was before
    }

    size_t send_line(const void* data, size_t size) override
    {
        // TODO: select loop with timeout
        struct iovec todo[2] = {
            { (void*)data, size },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != size + 1)
            throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
        return size + 1;
    }

    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override
    {
        // TODO: select loop with timeout
        fd.sendfile(out, offset, size);
        acct::plain_data_read_count.incr();
        // iotrace::trace_file(dirfd, offset, size, "streamed data");
        return size;
    }

    size_t send_buffer(const void* data, size_t size) override
    {
        // TODO: select loop with timeout
        // TODO: retry instead of throw
        out.write_all_or_throw(data, size);
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

    size_t send_from_pipe(int fd) override
    {
        // TODO: select loop with timeout
#if defined(__linux__)
#ifdef HAVE_SPLICE
        // Try splice
        ssize_t res = splice(subproc.get_stdout(), NULL, *m_nextfd, NULL, 4096*2, SPLICE_F_MORE);
        if (res >= 0)
            return res;

        if (errno != EINVAL)
            throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");

        // Splice is not supported: pass it on to the traditional method
#endif
#endif
        // Fall back to read/write

        // Read data from child
        char buf[4096 * 8];
        ssize_t res = read(fd, buf, 4096 * 8);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        if (res == 0)
            return 0;

        // Pass it on
        return send_buffer(buf, res);
    }
};


class AbstractStreamOutput: public StreamOutput
{
    AbstractOutputFile& out;

public:
    AbstractStreamOutput(AbstractOutputFile& out)
        : out(out)
    {
    }

    size_t send_line(const void* data, size_t size) override
    {
        out.write(data, size);
        out.write("\n", 1);
        return size + 1;
    }

    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override
    {
        constexpr size_t buf_size = 4096 * 16;
        char buf[buf_size];
        size_t pos = 0;

        while (pos < size)
        {
            size_t done = fd.pread(buf, std::min(buf_size, size - pos), offset + pos);
            if (done == 0)
                throw std::runtime_error(out.name() + ": read only " + std::to_string(pos) + "/" + std::to_string(size) + " bytes");
            out.write(buf, done);
            pos += done;
        }

        return size;
    }

    size_t send_buffer(const void* data, size_t size) override
    {
        out.write(data, size);
        return size;
    }

    size_t send_from_pipe(int fd) override
    {
        // Read a small chunk of data from child, to trigger data_start_hook
        char buf[4096 * 8];
        ssize_t res = read(fd, buf, 4096 * 8);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot read from pipe");
        if (res == 0)
            return 0;
        out.write(buf, res);
        return res;
    }
};

std::unique_ptr<StreamOutput> StreamOutput::create(NamedFileDescriptor& out)
{
    return std::unique_ptr<StreamOutput>(new ConcreteStreamOutput(out));
}

std::unique_ptr<StreamOutput> StreamOutput::create(NamedFileDescriptor& out, unsigned timeout_ms)
{
    return std::unique_ptr<StreamOutput>(new ConcreteTimeoutStreamOutput(out, timeout_ms));
}

std::unique_ptr<StreamOutput> StreamOutput::create(AbstractOutputFile& out)
{
    return std::unique_ptr<StreamOutput>(new AbstractStreamOutput(out));
}

}
}
