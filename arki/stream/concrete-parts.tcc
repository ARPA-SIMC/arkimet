#ifndef ARKI_STREAM_CONCRETE_PARTS_TCC
#define ARKI_STREAM_CONCRETE_PARTS_TCC

#include "arki/stream/fwd.h"
#include "arki/utils/sys.h"
#include "filter.h"
#include "loops.h"
#include "abstract.h"
#include "concrete.h"
#include <poll.h>
#include <string>
#include <functional>
#include <system_error>
#include "loops-send.tcc"

namespace arki {
namespace stream {

/**
 * Event loop addon that collects stderr from the filter process into the
 * `errors` buffer on the FilterProcess object
 */
template<typename Backend>
struct CollectFilterStderr
{
    FilterProcess& filter_process;
    pollfd* pfd_filter_stderr;
    std::array<uint8_t, 256> buffer;

    CollectFilterStderr(BaseStreamOutput& stream) : filter_process(*stream.filter_process) {}

    void set_output(pollfd* pollinfo)
    {
        pfd_filter_stderr = &pollinfo[POLLINFO_FILTER_STDERR];
        pfd_filter_stderr->fd = filter_process.cmd.get_stderr();
        pfd_filter_stderr->events = POLLIN;
    }

    void transfer_available_stderr()
    {
        ssize_t res = Backend::read(filter_process.cmd.get_stderr(), buffer.data(), buffer.size());
        trace_streaming("  read stderr → %d %.*s\n", (int)res, (int)res, (const char*)stderr_buffer);
        if (res == 0)
        {
            filter_process.cmd.close_stderr();
            pfd_filter_stderr->fd = -1;
        }
        else if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            else
                throw std::system_error(errno, std::system_category(), "cannot read data from pipe stderr");
        }
        else
        {
            filter_process.errors.write((const char*)buffer.data(), res);
            if (filter_process.errors.bad())
                throw std::system_error(errno, std::system_category(), "cannot store filter stderr in memory buffer");
        }
    }

    bool setup_poll()
    {
        return filter_process.cmd.get_stderr() != -1;
    }

    bool on_poll(SendResult& result)
    {
        if (pfd_filter_stderr->revents & POLLIN)
            transfer_available_stderr();

        if (pfd_filter_stderr->revents & (POLLERR | POLLHUP))
        {
            // Filter stderr closed its endpoint
            filter_process.cmd.close_stderr();
            pfd_filter_stderr->fd = -1;
        }

        return false;
    }
};


/**
 * Replacement for ToFilter when doing a flush
 *
 * Stops polling the filter stdin and doesn't send anything to it
 */
template<typename Backend>
struct Flusher
{
    BaseStreamOutput& stream;
    pollfd* pfd_filter_stdin;

    Flusher(BaseStreamOutput& stream)
        : stream(stream)
    {
    }

    void set_output(pollfd* pollinfo)
    {
        pfd_filter_stdin = &pollinfo[POLLINFO_FILTER_STDIN];
        pfd_filter_stdin->fd = -1;
        pfd_filter_stdin->events = 0;
    }

    bool setup_poll()
    {
        return false;
    }

    bool on_poll(SendResult& result)
    {
        return false;
    }
};


/**
 * Send data to the filter's input.
 *
 * This is a wrapper to ToPipe subclass which adds management for the filter
 * stdin's file descriptor
 */
template<typename Backend, typename ToPipe>
struct ToFilter
{
    BaseStreamOutput& stream;
    core::NamedFileDescriptor filter_stdin;
    pollfd* pfd_filter_stdin;
    ToPipe to_pipe;

    ToFilter(BaseStreamOutput& stream, ToPipe&& to_pipe)
        : stream(stream), filter_stdin(stream.filter_process->cmd.get_stdin(), "filter stdin"), to_pipe(std::move(to_pipe))
    {
    }

    void set_output(pollfd* pollinfo)
    {
        pfd_filter_stdin = &pollinfo[POLLINFO_FILTER_STDIN];
        pfd_filter_stdin->fd = stream.filter_process->cmd.get_stdin();
        pfd_filter_stdin->events = POLLOUT;
    }

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult feed_filter_stdin()
    {
        auto pre = to_pipe.pos;
        auto res = to_pipe.transfer_available(filter_stdin);
        this->stream.filter_process->size_stdin += to_pipe.pos - pre;
        return res;
    }

    bool setup_poll()
    {
        return stream.filter_process->cmd.get_stdin() != -1;
    }

    bool on_poll(SendResult& result)
    {
        if (pfd_filter_stdin->revents & POLLOUT)
        {
            switch (this->feed_filter_stdin())
            {
                case TransferResult::DONE:
                    // We have written everything, stop polling
                    return true;
                case TransferResult::EOF_SOURCE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_DEST:
                    // Filter closed stdin
                    throw std::runtime_error("filter process closed its input pipe while we still have data to process");
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }

        if (pfd_filter_stdin->revents & (POLLERR | POLLHUP))
        {
            // result.flags |= SendResult::SEND_PIPE_EOF_DEST; // TODO: signal this somehow?
            stream.filter_process->cmd.close_stdin();
            pfd_filter_stdin->fd = -1;
        }

        return false;
    }
};

/**
 * Base class for the logic of sending data from the filter's output to the
 * destination
 */
template<typename Backend>
struct FromFilter
{
    BaseStreamOutput& stream;
    pollfd* pfd_filter_stdout;
    std::function<void(size_t)> progress_callback;
    std::string out_name;
    bool filter_stdout_available = false;

    FromFilter(BaseStreamOutput& stream) : stream(stream) {}
    FromFilter(const FromFilter&) = default;
    FromFilter(FromFilter&&) = default;

    void set_output(pollfd* pollinfo)
    {
        pfd_filter_stdout = &pollinfo[POLLINFO_FILTER_STDOUT];
        pfd_filter_stdout->fd = stream.filter_process->cmd.get_stdout();
        pfd_filter_stdout->events = POLLIN;
    }

    bool setup_poll()
    {
        if (filter_stdout_available)
            pfd_filter_stdout->events = 0;
        else
            pfd_filter_stdout->events = POLLIN;

        trace_streaming("  FromFilter.setup_poll: filter_stdout: %d:%x\n", stream.filter_process->cmd.get_stdout(), (int)pfd_filter_stdout->events);
        return stream.filter_process->cmd.get_stdout() != -1;
    }

    bool on_poll(SendResult& result)
    {
        if (pfd_filter_stdout->revents & POLLIN)
            filter_stdout_available = true;

        return false;
    }
};

template<typename Backend>
struct FromFilterConcrete : public FromFilter<Backend>
{
    core::NamedFileDescriptor& out_fd;
    pollfd* pfd_destination;
    bool destination_available = false;
    FromFilterConcrete(ConcreteStreamOutputBase<Backend>& stream) : FromFilter<Backend>(stream), out_fd(*stream.out) {}
    FromFilterConcrete(const FromFilterConcrete&) = default;
    FromFilterConcrete(FromFilterConcrete&&) = default;

    void set_output(pollfd* pollinfo)
    {
        FromFilter<Backend>::set_output(pollinfo);
        this->out_name = out_fd.name();
        this->pfd_destination = &pollinfo[POLLINFO_DESTINATION];
        this->pfd_destination->fd = out_fd;
        this->pfd_destination->events = POLLOUT;
    }

    bool setup_poll()
    {
        bool res = FromFilter<Backend>::setup_poll();

        if (destination_available)
            pfd_destination->events = 0;
        else
            pfd_destination->events = POLLOUT;

        return res;
    }

    bool on_poll(SendResult& result)
    {
        bool done = FromFilter<Backend>::on_poll(result);

        if (this->pfd_destination->revents & POLLOUT)
            destination_available = true;

        if (this->pfd_destination->revents & (POLLERR | POLLHUP))
        {
            // Destination closed its endpoint, stop here
            result.flags |= SendResult::SEND_PIPE_EOF_DEST;
            return true;
        }

        return done;
    }
};

template<typename Backend>
struct FromFilterSplice : public FromFilterConcrete<Backend>
{
    using FromFilterConcrete<Backend>::FromFilterConcrete;

    TransferResult transfer_available_output()
    {
#ifndef HAVE_SPLICE
        throw SpliceNotAvailable();
#else
        // Try splice
        ssize_t res = Backend::splice(this->stream.filter_process->cmd.get_stdout(), NULL, this->out_fd, NULL, TransferBuffer::size * 128, SPLICE_F_MORE | SPLICE_F_NONBLOCK);
        trace_streaming("  splice stdout → %d\n", (int)res);
        if (res > 0)
        {
            if (this->stream.progress_callback)
                this->stream.progress_callback(res);
            this->stream.filter_process->size_stdout += res;
            return TransferResult::WOULDBLOCK;
        } else if (res == 0) {
            return TransferResult::EOF_SOURCE;
        } else {
            if (errno == EINVAL)
            {
                throw SpliceNotAvailable();
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return TransferResult::WOULDBLOCK;
            } else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot splice data to stream from a pipe");
        }
#endif
    }

    bool on_poll(SendResult& result)
    {
        bool done = FromFilterConcrete<Backend>::on_poll(result);

        auto& cmd = this->stream.filter_process->cmd;
        trace_streaming("  FromFilterSplice.on_poll [%d %d] %d %d\n", cmd.get_stdout(), cmd.get_stderr(), this->filter_stdout_available, this->destination_available);

        if (cmd.get_stdout() == -1 && cmd.get_stderr() == -1)
            return true;

        if (this->filter_stdout_available and this->destination_available)
        {
            trace_streaming("  FromFilterSplice.on_poll: source and destination available: transfer data\n");
            this->filter_stdout_available = false;
            this->destination_available = false;

            switch (this->transfer_available_output())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // Filter closed stdout, evidently we are done
                    done = true;
                    break;
                case TransferResult::EOF_DEST:
                    // Destination closed its pipe
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    done = true;
                    break;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }

        if (this->pfd_filter_stdout->revents & (POLLERR | POLLHUP))
        {
            trace_streaming("  FromFilterSplice.on_poll: filter stdout closed\n");
            // Filter stdout closed its endpoint
            this->stream.filter_process->cmd.close_stdout();
            this->pfd_filter_stdout->fd = -1;
        }

        return done;
    }
};

template<typename Backend>
struct FromFilterReadWrite : public FromFilterConcrete<Backend>
{
    TransferBuffer buffer;
    BufferToPipe<Backend> to_output;
    core::NamedFileDescriptor& destination;

    FromFilterReadWrite(ConcreteStreamOutputBase<Backend>& stream)
        : FromFilterConcrete<Backend>(stream), to_output(nullptr, 0), destination(*stream.out)
    {
        buffer.allocate();
    }
    FromFilterReadWrite(const FromFilterReadWrite&) = default;
    FromFilterReadWrite(FromFilterReadWrite&&) = default;

    TransferResult transfer_available_output_read()
    {
        to_output.reset(buffer.buf, 0);
        ssize_t res = Backend::read(this->stream.filter_process->cmd.get_stdout(), buffer, buffer.size);
        trace_streaming("  read stdout → %d %.*s\n", (int)res, std::max((int)res, 0), (const char*)buffer);
        if (res == 0)
            return TransferResult::EOF_SOURCE;
        else if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else
                throw std::system_error(errno, std::system_category(), "cannot read data from filter stdout");
        }
        else
        {
            to_output.reset(buffer.buf, res);
            this->stream.filter_process->size_stdout += res;
            return TransferResult::WOULDBLOCK;
        }
    }

    TransferResult transfer_available_output_write()
    {
        auto pre = to_output.pos;
        auto res = to_output.transfer_available(destination);
        if (this->stream.progress_callback)
            this->stream.progress_callback(pre - to_output.pos);
        return res;
    }

    bool setup_poll()
    {
        bool res = FromFilter<Backend>::setup_poll();
        res = res or (to_output.size > 0);
        return res;
    }

    bool on_poll(SendResult& result)
    {
        bool done = FromFilterConcrete<Backend>::on_poll(result);

        trace_streaming("  FromFilterReadWrite.on_poll %zd\n", to_output.size);

        // TODO: this could append to the output buffer to chunk together small writes
        if (to_output.size == 0 && this->filter_stdout_available)
        {
            this->filter_stdout_available = false;
            switch (transfer_available_output_read())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    this->stream.filter_process->cmd.close_stdout();
                    break;
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
                    if (this->stream.filter_process->cmd.get_stdout() == -1)
                        // We are done if filter_stdout is closed
                        done = true;
                    break;
                case TransferResult::EOF_SOURCE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_DEST:
                    // Destination closed its pipe
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    done = true;
                    break;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }

        if (this->pfd_filter_stdout->revents & (POLLERR | POLLHUP))
        {
            // Filter stdout closed its endpoint
            this->stream.filter_process->cmd.close_stdout();
            this->pfd_filter_stdout->fd = -1;
        }

        return done;
    }
};


template<typename Backend>
struct FromFilterAbstract : public FromFilter<Backend>
{
    TransferBuffer buffer;

    FromFilterAbstract(AbstractStreamOutput<Backend>& stream) : FromFilter<Backend>(stream) { buffer.allocate(); }
    FromFilterAbstract(const FromFilterAbstract&) = default;
    FromFilterAbstract(FromFilterAbstract&&) = default;

    void set_output(pollfd* pollinfo)
    {
        FromFilter<Backend>::set_output(pollinfo);
        this->out_name = "output";
    }

    TransferResult transfer_available_output()
    {
        ssize_t res = Backend::read(this->stream.filter_process->cmd.get_stdout(), buffer, buffer.size);
        trace_streaming("  read stdout %d → %d %.*s\n", this->stream.filter_process->cmd.get_stdout(), (int)res, std::max((int)res, 0), (const char*)buffer);
        if (res == 0)
            return TransferResult::EOF_SOURCE;
        else if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else
                throw std::system_error(errno, std::system_category(), "cannot read data from filter stdout");
        }
        else
        {
            AbstractStreamOutput<Backend>* stream = reinterpret_cast<AbstractStreamOutput<Backend>*>(&(this->stream));
            stream->_write_output_buffer(buffer.buf, res);
            this->stream.filter_process->size_stdout += res;
            return TransferResult::WOULDBLOCK;
        }
    }

    bool on_poll(SendResult& result)
    {
        bool done = FromFilter<Backend>::on_poll(result);

        trace_streaming("  FromFilterAbstract.on_poll\n");

        // TODO: this could append to the output buffer to chunk together small writes
        if (this->filter_stdout_available)
        {
            this->filter_stdout_available = false;
            switch (transfer_available_output())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // Filter closed stdout, stop polling it
                    done = true;
                    break;
                case TransferResult::EOF_DEST:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }

        if (this->pfd_filter_stdout->revents & (POLLERR | POLLHUP))
        {
            // Filter stdout closed its endpoint
            this->stream.filter_process->cmd.close_stdout();
            this->pfd_filter_stdout->fd = -1;
        }

        return done;
    }
};


}
}

#endif
