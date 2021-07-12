#ifndef ARKI_STREAM_CONCRETE_TCC
#define ARKI_STREAM_CONCRETE_TCC

#include "concrete.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/sendfile.h>
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

template<typename Backend>
struct Sender
{
    ConcreteStreamOutputBase<Backend>& stream;
    stream::SendResult result;

    Sender(ConcreteStreamOutputBase<Backend>& stream)
        : stream(stream)
    {
    }
};

template<typename Backend, template<typename> class ToOutput>
struct BufferSender: public Sender<Backend>
{
    ToOutput<Backend> to_output;
    pollfd pollinfo;

    BufferSender(ConcreteStreamOutputBase<Backend>& stream, const void* data, size_t size)
        : Sender<Backend>(stream), to_output(data, size, pollinfo, stream.out->name())
    {
        pollinfo.fd = *stream.out;
        pollinfo.events = POLLOUT;
    }

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available()
    {
        if (this->stream.data_start_callback)
        {
            this->result += this->stream.fire_data_start_callback();
            return TransferResult::WOULDBLOCK;
        }
        return to_output.transfer_available();
    }

    stream::SendResult loop()
    {
        while (true)
        {
            pollinfo.revents = 0;
            int res = Backend::poll(&pollinfo, 1, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed on " + this->stream.out->name());
            if (res == 0)
                throw TimedOut("write on " + this->stream.out->name() + " timed out");
            if (pollinfo.revents & POLLERR)
                return SendResult::SEND_PIPE_EOF_DEST;
            if (pollinfo.revents & POLLOUT)
            {
                switch (transfer_available())
                {
                    case TransferResult::DONE:
                        return this->result;
                    case TransferResult::EOF_SOURCE:
                        this->result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                        return this->result;
                    case TransferResult::EOF_DEST:
                        this->result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                        return this->result;
                    case TransferResult::WOULDBLOCK:
                        break;
                }
            }
            else
                throw std::runtime_error("unsupported revents values when polling " + this->stream.out->name());
        }
    }
};

template<typename Backend, template<typename> class ToFilter>
struct BufferSenderFiltered
{
    ConcreteStreamOutputBase<Backend>& stream;
    stream::SendResult result;
    ToFilter<Backend> to_filter;
    pollfd pollinfo[3];
    bool filter_stdout_available = false;
    bool destination_available = false;

    BufferSenderFiltered(ConcreteStreamOutputBase<Backend>& stream, const void* data, size_t size)
        : stream(stream), to_filter(data, size, pollinfo[0], "filter stdin")
    {
        pollinfo[0].fd = stream.filter_process->cmd.get_stdin();
        pollinfo[0].events = POLLOUT;
        pollinfo[1].fd = stream.filter_process->cmd.get_stdout();
        pollinfo[1].events = POLLIN;
        pollinfo[2].fd = *stream.out;
        pollinfo[2].events = POLLOUT;
    }
    virtual ~BufferSenderFiltered() {}

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult feed_filter_stdin()
    {
        auto pre = to_filter.pos;
        auto res = to_filter.transfer_available();
        stream.filter_process->size_stdin += to_filter.pos - pre;
        return res;
    }

    virtual bool on_poll() = 0;

    stream::SendResult loop()
    {
        while (true)
        {
            // Suppose se use a /dev/null filter. Destination fd is always ready,
            // filter stdout never has data. We end up busy-looping, because with
            // destination fd ready, pipe has always something to report.
            //
            // We use flags to turn level-triggered events into edge-triggered
            // events for the filter output and destination pipelines
            //
            // In other words, we skip monitoring availability until we drain
            // the buffers
            if (filter_stdout_available)
                pollinfo[1].fd = -1;
            else
                pollinfo[1].fd = stream.filter_process->cmd.get_stdout();
            if (destination_available)
                pollinfo[2].fd = -1;
            else
                pollinfo[2].fd = *stream.out;

            // filter stdin     | filter stdout       | destination fd
            this->pollinfo[0].revents = this->pollinfo[1].revents = this->pollinfo[2].revents = 0;
            int res = Backend::poll(this->pollinfo, 3, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed on " + this->stream.out->name());
            if (res == 0)
                throw TimedOut("write on " + this->stream.out->name() + " timed out");

            fprintf(stderr, "POLL: %d:%d %d:%d %d:%d\n",
                    this->pollinfo[0].fd, this->pollinfo[0].revents,
                    this->pollinfo[1].fd, this->pollinfo[1].revents,
                    this->pollinfo[2].fd, this->pollinfo[2].revents);

            if (this->pollinfo[0].revents & POLLERR)
            {
                // TODO: child process closed stdin but we were still writing on it
                this->result.flags |= SendResult::SEND_PIPE_EOF_DEST; // This is not the right return code
                this->pollinfo[0].fd = -this->pollinfo[0].fd;
            }

            if (this->pollinfo[1].revents & POLLERR)
            {
                // TODO: child process closed stdout but we were still writing on stdin
                this->result.flags |= SendResult::SEND_PIPE_EOF_DEST; // This is not the right return code
                this->pollinfo[1].fd = -this->pollinfo[1].fd;
            }

            if (this->pollinfo[2].revents & POLLERR)
            {
                // Destination closed its endpoint, stop here
                // TODO: stop writing to filter stdin and drain filter stdout?
                this->result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                return this->result;
            }

            if (this->pollinfo[0].revents & POLLOUT)
            {
                switch (this->feed_filter_stdin())
                {
                    case TransferResult::DONE:
                        // We have written everything, stop polling
                        return this->result;
                    case TransferResult::EOF_SOURCE:
                        throw std::runtime_error("unexpected result from feed_filter_stdin");
                    case TransferResult::EOF_DEST:
                        // Filter closed stdin
                        throw std::runtime_error("filter process closed its input pipe while we still have data to process");
                    case TransferResult::WOULDBLOCK:
                        break;
                }
            }

            if (this->pollinfo[1].revents & POLLIN)
                filter_stdout_available = true;

            if (this->pollinfo[2].revents & POLLOUT)
                destination_available = true;

            if (on_poll())
                return this->result;
        }
    }
};

template<typename Backend, template<typename> class ToFilter>
struct BufferSenderFilteredSplice : public BufferSenderFiltered<Backend, ToFilter>
{
    using BufferSenderFiltered<Backend, ToFilter>::BufferSenderFiltered;

    TransferResult transfer_available_output_splice()
    {
        if (this->stream.data_start_callback)
        {
            this->result += this->stream.fire_data_start_callback();
            return TransferResult::WOULDBLOCK;
        }

#ifndef HAVE_SPLICE
        throw std::runtime_error("BufferSenderFilteredSplice strategy chosen when splice() is not available");
#else
        while (true)
        {
            utils::Sigignore ignpipe(SIGPIPE);
            // Try splice
            ssize_t res = Backend::splice(this->pollinfo[1].fd, NULL, this->pollinfo[2].fd, NULL, TransferBuffer::size * 128, SPLICE_F_MORE | SPLICE_F_NONBLOCK);
            fprintf(stderr, "  splice stdout → %d\n", (int)res);
            if (res > 0)
            {
                if (this->stream.progress_callback)
                    this->stream.progress_callback(res);
                this->stream.filter_process->size_stdout += res;
            } else if (res == 0) {
                return TransferResult::EOF_SOURCE;
            }
            else if (res < 0)
            {
                if (errno == EINVAL)
                {
                    // has_splice = false;
                    // continue;
                    throw std::system_error(errno, std::system_category(), "splice became unavailable during streaming");
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return TransferResult::WOULDBLOCK;
                } else if (errno == EPIPE) {
                    return TransferResult::EOF_DEST;
                } else
                    throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
            }
        }
#endif
    }

    bool on_poll() override
    {
        if (this->filter_stdout_available and this->destination_available)
        {
            this->filter_stdout_available = false;
            this->destination_available = false;

            switch (this->transfer_available_output_splice())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // Filter closed stdout
                    this->result.flags |= SendResult::SEND_PIPE_EOF_SOURCE; // This is not the right return code
                    return true;
                case TransferResult::EOF_DEST:
                    // Destination closed its pipe
                    this->result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    return true;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }
        return false;
    }
};

template<typename Backend, template<typename> class ToFilter>
struct BufferSenderFilteredReadWrite : public BufferSenderFiltered<Backend, ToFilter>
{
    TransferBuffer buffer;
    BufferToOutput<Backend> to_output;

    BufferSenderFilteredReadWrite(ConcreteStreamOutputBase<Backend>& stream, const void* data, size_t size)
        : BufferSenderFiltered<Backend, ToFilter>(stream, data, size), to_output(nullptr, 0, this->pollinfo[2], stream.out->name())
    {
        buffer.allocate();
    }

    TransferResult transfer_available_output_read()
    {
        // Fall back to read/write
        buffer.allocate();
        to_output.reset(buffer.buf, 0);
        ssize_t res = Backend::read(this->pollinfo[1].fd, buffer, buffer.size);
        fprintf(stderr, "  read stdout → %d %.*s\n", (int)res, (int)res, (const char*)buffer);
        if (res == 0)
            return TransferResult::EOF_SOURCE;
        else if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else
                throw std::system_error(errno, std::system_category(), "cannot read data from pipe input");
        }
        else
        {
            to_output.reset(buffer.buf, res);
            return TransferResult::WOULDBLOCK;
        }
    }

    TransferResult transfer_available_output_write()
    {
        auto pre = to_output.pos;
        auto res = to_output.transfer_available();
        if (this->stream.progress_callback)
            this->stream.progress_callback(pre - to_output.pos);
        return res;
    }

    bool on_poll() override
    {
        // TODO: this could append to the output buffer to chunk together small writes
        if (to_output.size == 0 && this->filter_stdout_available)
        {
            this->filter_stdout_available = false;
            switch (transfer_available_output_read())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // Filter closed stdout
                    this->result.flags |= SendResult::SEND_PIPE_EOF_SOURCE; // This is not the right return code
                    return true;
                case TransferResult::EOF_DEST:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }

        if (to_output.size > 0 && this->destination_available)
        {
            this->destination_available = false;
            switch (transfer_available_output_write())
            {
                case TransferResult::DONE:
                    break;
                case TransferResult::EOF_SOURCE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_DEST:
                    // Destination closed its pipe
                    this->result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    return true;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }
        return false;
    }
};


template<typename Backend>
std::string ConcreteStreamOutputBase<Backend>::name() const { return out->name(); }

template<typename Backend>
uint32_t ConcreteStreamOutputBase<Backend>::wait_writable()
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
ConcreteStreamOutputBase<Backend>::ConcreteStreamOutputBase(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms)
    : out(out), timeout_ms(timeout_ms)
{
    this->timeout_ms = timeout_ms;
    orig_fl = fcntl(*out, F_GETFL);
    if (orig_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for " + out->name());
    if (fcntl(*out, F_SETFL, orig_fl | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for " + out->name());

    pollinfo.fd = *out;
    pollinfo.events = POLLOUT;
}

template<typename Backend>
ConcreteStreamOutputBase<Backend>::~ConcreteStreamOutputBase()
{
    // If out is still open, reset as it was before
    if (*out != -1)
        fcntl(*out, F_SETFL, orig_fl);
}

template<typename Backend>
stream::SendResult ConcreteStreamOutputBase<Backend>::_write_output_buffer(const void* data, size_t size)
{
    SendResult result;
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
}

template<typename Backend>
void ConcreteStreamOutputBase<Backend>::start_filter(const std::vector<std::string>& command)
{
    BaseStreamOutput::start_filter(command);

    // Check if we can splice between the filter stdout and our output
#ifndef HAVE_SPLICE
    has_splice = false;
#else
    has_splice = Backend::splice(filter_process->cmd.get_stdout(), NULL, *out, NULL, 0, SPLICE_F_MORE | SPLICE_F_NONBLOCK) == 0;
#endif
}

template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        if (has_splice)
        {
            BufferSenderFilteredSplice<Backend, BufferToOutput> sender(*this, data, size);
            return sender.loop();
        } else {
            BufferSenderFilteredReadWrite<Backend, BufferToOutput> sender(*this, data, size);
            return sender.loop();
        }
    } else {
        BufferSender<Backend, BufferToOutput> sender(*this, data, size);
        return sender.loop();
    }
}

template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_line(const void* data, size_t size)
{
    SendResult result;
    // TODO: error: an empty buffer should send a newline
    if (size == 0)
        return result;

    if (filter_process)
    {
        if (has_splice)
        {
            BufferSenderFilteredSplice<Backend, LineToOutput> sender(*this, data, size);
            return sender.loop();
        } else {
            BufferSenderFilteredReadWrite<Backend, LineToOutput> sender(*this, data, size);
            return sender.loop();
        }
    } else {
        BufferSender<Backend, LineToOutput> sender(*this, data, size);
        return sender.loop();
    }
    return result;
}


template<typename Backend>
SendResult ConcreteStreamOutputBase<Backend>::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
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
        ssize_t res = Backend::pread(fd, buffer, std::min(size, buffer.size), offset);
        // FIXME: this can EAGAIN
        if (res == -1)
            fd.throw_error("cannot pread");
        else if (res == 0)
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

    // TODO: redo with a proper event loop
    int outfd;
    if (filter_process)
        outfd = filter_process->cmd.get_stdin();
    else
        outfd = *out;

    bool has_sendfile = true;
    size_t written = 0;
    while (size > 0)
    {
        if (has_sendfile)
        {
            utils::Sigignore ignpipe(SIGPIPE);
            ssize_t res = Backend::sendfile(outfd, fd, &offset, size - written);
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
            }
        } else {
            ssize_t res = Backend::pread(fd, buffer, std::min(size - written, buffer.size), offset);
            if (res == -1)
                fd.throw_error("cannot pread");
            else if (res == 0)
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
std::pair<size_t, SendResult> ConcreteStreamOutputBase<Backend>::send_from_pipe(int fd)
{
    bool src_nonblock = is_nonblocking(fd);
    size_t sent = 0;
    SendResult result;

    TransferBuffer buffer;
    if (data_start_callback)
    {
        // If we have data_start_callback, we need to do a regular read/write
        // cycle before attempting the handover to splice, to see if there
        // actually are data to read and thus output to generate.
        buffer.allocate();
        ssize_t res = Backend::read(fd, buffer, buffer.size);
        // FIXME: this can EAGAIN and it's not managed with a retry
        // there isn't much sense in exiting with SEND_PIPE_EAGAIN_SOURCE
        if (res < 0)
        {
            if (errno == EAGAIN) {
                result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                return std::make_pair(sent, result);
            } else
                throw std::system_error(errno, std::system_category(), "cannot read data to stream from a pipe");
        }
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            return std::make_pair(sent, result);
        }

        // If we get some output, then we *do* call data_start_callback, stream
        // it out, and proceed with the splice handover attempt
        if (data_start_callback)
            result += fire_data_start_callback();
        result += _write_output_buffer(buffer, res);
        sent += res;
        if (progress_callback)
            progress_callback(res);
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
                sent += res;
                if (progress_callback)
                    progress_callback(res);
            } else if (res == 0) {
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
            buffer.allocate();
            ssize_t res = Backend::read(fd, buffer, buffer.size);
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
            {
                if (data_start_callback)
                    result += fire_data_start_callback();
                result += _write_output_buffer(buffer, res);
                if (progress_callback)
                    progress_callback(res);
            } else {
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

    return std::make_pair(sent, result);
}

}
}

#endif
