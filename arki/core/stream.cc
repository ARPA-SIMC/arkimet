#include "stream.h"
#include "file.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/libconfig.h"
#include <system_error>
#include <cerrno>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <poll.h>

using namespace arki::utils;

namespace arki {
namespace core {

StreamOutput::~StreamOutput()
{
}


class BaseStreamOutput : public StreamOutput
{
protected:
    std::function<void(size_t)> progress_callback;

public:
    void set_progress_callback(std::function<void(size_t)> f) override
    {
        progress_callback = f;
    }
};


class ConcreteStreamOutput: public BaseStreamOutput
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
        if (progress_callback)
            progress_callback(res);
        return size + 1;
    }

    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override
    {
        fd.sendfile(out, offset, size);
        acct::plain_data_read_count.incr();
        // iotrace::trace_file(dirfd, offset, size, "streamed data");
        if (progress_callback)
            progress_callback(size);
        return size;
    }

    size_t send_buffer(const void* data, size_t size) override
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

    size_t send_from_pipe(int fd) override
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
};


namespace {

struct TransferBuffer
{
    constexpr static size_t size = 40960;
    char* buf = nullptr;

    TransferBuffer() = default;
    TransferBuffer(const TransferBuffer&) = delete;
    TransferBuffer(TransferBuffer&&) = delete;
    ~TransferBuffer()
    {
        delete[] buf;
    }
    TransferBuffer& operator=(const TransferBuffer&) = delete;
    TransferBuffer& operator=(TransferBuffer&&) = delete;

    void allocate()
    {
        if (buf)
            return;
        buf = new char[size];
    }

    operator char*() { return buf; }
};

size_t constexpr TransferBuffer::size;

}


class ConcreteTimeoutStreamOutput: public BaseStreamOutput
{
    sys::NamedFileDescriptor& out;
    unsigned timeout_ms;
    int orig_fl = -1;
    pollfd pollinfo;

    void wait_readable()
    {
        pollinfo.revents = 0;
        int res = ::poll(&pollinfo, 1, timeout_ms);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "poll failed on " + out.name());
        if (res == 0)
            throw StreamTimedOut("write on " + out.name() + " timed out");
        if (pollinfo.revents & POLLERR)
            throw StreamClosed("peer on " + out.name() + " has closed the connection");
        if (pollinfo.revents & POLLOUT)
            return;
        throw std::runtime_error("unsupported revents values when polling " + out.name());
    }

public:
    ConcreteTimeoutStreamOutput(sys::NamedFileDescriptor& out, unsigned timeout_ms)
        : out(out), timeout_ms(timeout_ms)
    {
        orig_fl = fcntl(out, F_GETFL);
        if (orig_fl < 0)
            throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for " + out.name());
        if (fcntl(out, F_SETFL, orig_fl | O_NONBLOCK) < 0)
            throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for " + out.name());

        pollinfo.fd = out;
        pollinfo.events = POLLOUT;
    }
    ~ConcreteTimeoutStreamOutput()
    {
        // If out is still open, reset as it was before
        if (out != -1)
            fcntl(out, F_SETFL, orig_fl);
    }

    size_t send_line(const void* data, size_t size) override
    {
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
                    else
                        throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size + 1) + " bytes to " + out.name());
                }
                pos += res;
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
                    else
                        throw std::system_error(errno, std::system_category(), "cannot write 1 byte to " + out.name());
                }
                pos += res;
                if (progress_callback)
                    progress_callback(res);
            } else
                break;
            wait_readable();
        }
        return pos;
    }

    size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) override
    {
        TransferBuffer buffer;
        bool has_sendfile = true;
        size_t written = 0;
        while (true)
        {
            if (has_sendfile)
            {
                ssize_t res = ::sendfile(out, fd, &offset, size - written);
                if (res < 0)
                {
                    if (errno == EINVAL || errno == ENOSYS)
                    {
                        has_sendfile = false;
                        buffer.allocate();
                    }
                    else if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        res = 0;
                        if (progress_callback)
                            progress_callback(res);
                    }
                    else
                        throw std::system_error(errno, std::system_category(), "cannot sendfile() " + std::to_string(size) + " bytes to " + out.name());
                } else {
                    if (progress_callback)
                        progress_callback(res);
                    written += res;
                }
            } else {
                size_t res = out.pread(buffer, std::min(size - written, buffer.size), offset);
                send_buffer(buffer, res);
                offset += res;
                written += res;
                if (progress_callback)
                    progress_callback(res);
            }

            acct::plain_data_read_count.incr();

            if (written >= size)
                break;
            wait_readable();
            // iotrace::trace_file(dirfd, offset, size, "streamed data");
        }
        return written;
    }

    size_t send_buffer(const void* data, size_t size) override
    {
        size_t pos = 0;
        while (true)
        {
            ssize_t res = ::write(out, (const uint8_t*)data + pos, size - pos);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    res = 0;
                else
                    throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out.name());
            }

            pos += res;
            if (progress_callback)
                progress_callback(res);

            if (pos >= size)
                break;

            wait_readable();
        }
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
        TransferBuffer buffer;
        bool has_splice = true;
        size_t sent = 0;
        while (true)
        {
            if (has_splice)
            {
#if defined(__linux__) && defined(HAVE_SPLICE)
                // Try splice
                ssize_t res = splice(fd, NULL, out, NULL, 4096 * 8, SPLICE_F_MORE);
                if (res == 0)
                    return sent;
                else if (res < 0)
                {
                    if (errno == EINVAL)
                    {
                        has_splice = false;
                        buffer.allocate();
                        continue;
                    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        res = 0;
                    } else
                        throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
                }
                sent += res;
                if (progress_callback)
                    progress_callback(res);

#else
                // Splice is not supported: pass it on to the traditional method
                has_splice = false;
                buffer.allocate();
#endif
            } else {
                // Fall back to read/write
                ssize_t res = ::read(fd, buffer, buffer.size);
                if (res == 0)
                    return sent;
                if (res < 0)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        res = 0;
                    else
                        throw std::system_error(errno, std::system_category(), "cannot read data from pipe input");
                }
                if (res > 0)
                    sent += send_buffer(buffer, res);
                else
                {
                    if (progress_callback)
                        progress_callback(res);
                }
            }

            wait_readable();
        }
    }
};


class AbstractStreamOutput: public BaseStreamOutput
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
        if (progress_callback)
            progress_callback(size + 1);
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
            if (progress_callback)
                progress_callback(done);
        }

        return size;
    }

    size_t send_buffer(const void* data, size_t size) override
    {
        out.write(data, size);
        if (progress_callback)
            progress_callback(size);
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
        if (progress_callback)
            progress_callback(res);
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
