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

template<typename Backend, typename ToFilter, typename ToOutput>
struct SenderFiltered : public Sender
{
    ToFilter to_filter;
    ToOutput to_output;
    pollfd pollinfo[3];

    SenderFiltered(BaseStreamOutput& stream, ToFilter&& to_filter, ToOutput&& to_output)
        : Sender(stream), to_filter(std::move(to_filter)), to_output(std::move(to_output)) // "filter stdin", pollinfo[0], std::forward<Args>(args)...)
    {
        pollinfo[0].fd = stream.filter_process->cmd.get_stdin();
        pollinfo[0].events = POLLOUT;
        pollinfo[1].fd = stream.filter_process->cmd.get_stdout();
        pollinfo[1].events = POLLIN;
        pollinfo[2].fd = -1;
        pollinfo[2].events = 0;
        this->to_filter.set_output(core::NamedFileDescriptor(pollinfo[0].fd, "filter stdin"), pollinfo[0]);
        this->to_output.sender_for_data_start_callback = this;
        this->to_output.set_output(pollinfo[1], pollinfo[2]);
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
            to_output.setup_poll();

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

            if (to_output.on_poll(this->result))
                return this->result;
        }
    }
};

}
}

#endif
