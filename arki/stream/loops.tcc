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

template<typename Backend, typename ToFilter>
struct SenderFiltered : public Sender
{
    ToFilter to_filter;
    pollfd pollinfo[3];
    bool filter_stdout_available = false;
    bool destination_available = false;

    SenderFiltered(ConcreteStreamOutputBase<Backend>& stream, ToFilter&& to_filter)
        : Sender(stream), to_filter(std::move(to_filter)) // "filter stdin", pollinfo[0], std::forward<Args>(args)...)
    {
        pollinfo[0].fd = stream.filter_process->cmd.get_stdin();
        pollinfo[0].events = POLLOUT;
        pollinfo[1].fd = stream.filter_process->cmd.get_stdout();
        pollinfo[1].events = POLLIN;
        pollinfo[2].fd = *stream.out;
        pollinfo[2].events = POLLOUT;
        this->to_filter.set_output(core::NamedFileDescriptor(pollinfo[0].fd, "filter stdin"), pollinfo[0]);
    }
    virtual ~SenderFiltered() {}

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
                pollinfo[1].fd = this->stream.filter_process->cmd.get_stdout();
            if (destination_available)
                pollinfo[2].fd = -1;
            else
                pollinfo[2].fd = *reinterpret_cast<ConcreteStreamOutputBase<Backend>*>(&(this->stream))->out;

            // filter stdin     | filter stdout       | destination fd
            this->pollinfo[0].revents = this->pollinfo[1].revents = this->pollinfo[2].revents = 0;
            int res = Backend::poll(this->pollinfo, 3, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed");
            if (res == 0)
                throw TimedOut("streaming operations timed out");

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

template<typename Backend, typename ToFilter>
struct SenderFilteredSplice : public SenderFiltered<Backend, ToFilter>
{
    using SenderFiltered<Backend, ToFilter>::SenderFiltered;

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
                    throw SpliceNotAvailable();
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

template<typename Backend, typename ToFilter>
struct SenderFilteredReadWrite : public SenderFiltered<Backend, ToFilter>
{
//    using SenderFiltered<Backend, ToFilter>::SenderFiltered;

    TransferBuffer buffer;
    BufferToPipe<Backend> to_output;

    template<typename... Args>
    SenderFilteredReadWrite(ConcreteStreamOutputBase<Backend>& stream, Args&&... args)
        : SenderFiltered<Backend, ToFilter>(stream, std::forward<Args>(args)...),
          to_output(nullptr, 0)
    {
        buffer.allocate();
        to_output.set_output(*stream.out, this->pollinfo[2]);
    }

    TransferResult transfer_available_output_read()
    {
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

}
}

#endif
