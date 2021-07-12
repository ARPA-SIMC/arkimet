#ifndef ARKI_STREAM_LOOPS_TCC
#define ARKI_STREAM_LOOPS_TCC

#include "loops.h"
#include "filter.h"
#include "concrete.h"
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

template<typename Backend, typename ToOutput>
struct SenderDirect: public Sender
{
    ToOutput to_output;
    core::NamedFileDescriptor& out_fd;
    pollfd pollinfo;

    SenderDirect(ConcreteStreamOutputBase<Backend>& stream, ToOutput&& to_output)
        : Sender(stream), to_output(std::move(to_output)), out_fd(*stream.out)
    {
        pollinfo.fd = *stream.out;
        pollinfo.events = POLLOUT;
        this->to_output.sender_for_data_start_callback = this;
        this->to_output.set_output(*stream.out, pollinfo);
    }

    stream::SendResult loop()
    {
        while (true)
        {
            this->pollinfo.revents = 0;
            int res = Backend::poll(&this->pollinfo, 1, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed on " + out_fd.name());
            if (res == 0)
                throw TimedOut("write on " + out_fd.name() + " timed out");
            if (this->pollinfo.revents & POLLERR)
                return SendResult::SEND_PIPE_EOF_DEST;
            if (this->pollinfo.revents & POLLOUT)
            {
                switch (this->to_output.transfer_available())
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
                throw std::runtime_error("unsupported revents values when polling " + out_fd.name());
        }
    }
};

template<typename Backend, typename ToOutput>
struct FilteredBase : public Sender
{
    ToOutput to_output;
    TransferBuffer stderr_buffer;
    pollfd& pollinfo_stdout;
    pollfd& pollinfo_stderr;

    FilteredBase(BaseStreamOutput& stream, ToOutput&& to_output, pollfd& pollinfo_stdout, pollfd& pollinfo_stderr)
        : Sender(stream), to_output(std::move(to_output)), pollinfo_stdout(pollinfo_stdout), pollinfo_stderr(pollinfo_stderr)
    {
        pollinfo_stdout.fd = stream.filter_process->cmd.get_stdout();
        pollinfo_stdout.events = POLLIN;
        pollinfo_stderr.fd = stream.filter_process->cmd.get_stderr();
        pollinfo_stderr.events = POLLIN;
        stderr_buffer.allocate();
    }

    void transfer_available_stderr()
    {
        ssize_t res = Backend::read(pollinfo_stderr.fd, stderr_buffer, stderr_buffer.size);
        fprintf(stderr, "  read stderr → %d %.*s\n", (int)res, (int)res, (const char*)stderr_buffer);
        if (res == 0)
        {
            stream.filter_process->cmd.close_stderr();
            pollinfo_stderr.fd = -1;
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
            if (stream.filter_process->m_err)
            {
                stream.filter_process->m_err->write(stderr_buffer, res);
                if (stream.filter_process->m_err->bad())
                    throw std::system_error(errno, std::system_category(), "cannot store filter stderr in memory buffer");
            }
        }
    }

    bool on_poll()
    {
        if (this->pollinfo_stdout.revents & (POLLERR | POLLHUP))
        {
            // Filter stdout closed its endpoint
            stream.filter_process->cmd.close_stdin();
            pollinfo_stdout.fd = -1;
        }

        if (this->pollinfo_stderr.revents & (POLLERR | POLLHUP))
        {
            stream.filter_process->cmd.close_stderr();
            pollinfo_stderr.fd = -1;
        }

        if (this->pollinfo_stderr.revents & POLLOUT)
        {
            this->transfer_available_stderr();
        }

        return to_output.on_poll(this->result);
    }
};

template<typename Backend, typename ToFilter, typename ToOutput>
struct SenderFiltered : public FilteredBase<Backend, ToOutput>
{
    ToFilter to_filter;
    pollfd pollinfo[4];

    SenderFiltered(BaseStreamOutput& stream, ToFilter&& to_filter, ToOutput&& to_output)
        : FilteredBase<Backend, ToOutput>(stream, std::move(to_output), pollinfo[1], pollinfo[2]), to_filter(std::move(to_filter))
    {
        pollinfo[0].fd = stream.filter_process->cmd.get_stdin();
        pollinfo[0].events = POLLOUT;
        pollinfo[3].fd = -1;
        pollinfo[3].events = 0;
        this->to_filter.set_output(core::NamedFileDescriptor(pollinfo[0].fd, "filter stdin"), pollinfo[0]);
        this->to_output.sender_for_data_start_callback = this;
        this->to_output.set_output(pollinfo[1], pollinfo[3]);
    }

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult feed_filter_stdin()
    {
        auto pre = to_filter.pos;
        auto res = to_filter.transfer_available();
        this->stream.filter_process->size_stdin += to_filter.pos - pre;
        return res;
    }

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
            this->to_output.setup_poll();

            // filter stdin           | filter stdout             | filter stderr             | destination fd
            this->pollinfo[0].revents = this->pollinfo[1].revents = this->pollinfo[2].revents = this->pollinfo[3].revents = 0;
            int res = Backend::poll(this->pollinfo, 4, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed");
            if (res == 0)
                throw TimedOut("streaming operations timed out");

            fprintf(stderr, "POLL: %d:%d %d:%d %d:%d %d:%d\n",
                    this->pollinfo[0].fd, this->pollinfo[0].revents,
                    this->pollinfo[1].fd, this->pollinfo[1].revents,
                    this->pollinfo[2].fd, this->pollinfo[2].revents,
                    this->pollinfo[3].fd, this->pollinfo[3].revents);

            if (this->pollinfo[0].revents & POLLERR)
            {
                // TODO: child process closed stdin but we were still writing on it
                this->result.flags |= SendResult::SEND_PIPE_EOF_DEST; // This is not the right return code
                this->pollinfo[0].fd = -this->pollinfo[0].fd;
            }

            if (this->pollinfo[3].revents & POLLERR)
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

            if (this->on_poll())
                return this->result;
        }
    }
};

template<typename Backend, typename ToOutput>
struct FlushFilter : public FilteredBase<Backend, ToOutput>
{
    pollfd pollinfo[3];

    FlushFilter(BaseStreamOutput& stream, ToOutput&& to_output)
        : FilteredBase<Backend, ToOutput>(stream, std::move(to_output), pollinfo[0], pollinfo[1])
    {
        // Destination
        pollinfo[2].fd = -1;
        pollinfo[2].events = 0;
        this->to_output.sender_for_data_start_callback = this;
        this->to_output.set_output(pollinfo[1], pollinfo[2]);
    }

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
            this->to_output.setup_poll();

            // filter stdout          | filter stderr             | destination
            this->pollinfo[0].revents = this->pollinfo[1].revents = this->pollinfo[2].revents = 0;
            int res = Backend::poll(this->pollinfo, 3, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed");
            if (res == 0)
                throw TimedOut("streaming operations timed out");

            fprintf(stderr, "FlushFilter POLL: %d:%x %d:%x %d:%x\n",
                    this->pollinfo[0].fd, this->pollinfo[0].revents,
                    this->pollinfo[1].fd, this->pollinfo[1].revents,
                    this->pollinfo[2].fd, this->pollinfo[2].revents);

            if (this->pollinfo[2].revents & POLLERR)
            {
                // Destination closed its endpoint, stop here
                // TODO: stop writing to filter stdin and drain filter stdout?
                this->result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                return this->result;
            }

            if (this->on_poll())
                return this->result;
        }
    }
};

}
}

#endif
