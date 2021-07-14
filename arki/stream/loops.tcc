#ifndef ARKI_STREAM_LOOPS_TCC
#define ARKI_STREAM_LOOPS_TCC

#include "loops.h"
#include "filter.h"
#include "concrete.h"
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

/**
 * Event loop for sending data to an output file descriptor, without filters
 */
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
            trace_streaming("SenderDirect.POLL: %d %d:%d\n", res, this->pollinfo.fd, this->pollinfo.revents);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed on " + out_fd.name());
            if (res == 0)
                throw TimedOut("write on " + out_fd.name() + " timed out");
            if (this->pollinfo.revents & (POLLERR | POLLHUP))
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

/**
 * Base for event loops that manage a filter
 */
template<typename Backend, typename FromFilter>
struct FilterLoop : public Sender
{
    std::vector<PollElement*> poll_elements;
    /// pollfd structure described by the POLLINFO_* indices
    pollfd pollinfo[4];

    FilterLoop(BaseStreamOutput& stream, FromFilter&& from_filter)
        : Sender(stream)
    {
        for (unsigned i = 0; i < 4; ++i)
        {
            pollinfo[i].fd = -1;
            pollinfo[i].events = 0;
        }

        add_poll_element(new CollectFilterStderr<Backend>(stream));
        FromFilter* el = new FromFilter(std::move(from_filter));
        el->sender_for_data_start_callback = this;
        add_poll_element(el);
    }

    virtual ~FilterLoop()
    {
        for (auto& el: poll_elements)
            delete el;
    }

    void add_poll_element(PollElement* el)
    {
        el->set_output(pollinfo);
        poll_elements.emplace_back(el);
    }

    /**
     * Setup pollinfo before calling poll().
     *
     * Return true if there is still something to poll, false if all is done.
     */
    bool setup_poll()
    {
        bool needs_poll = false;
        for (auto& el : poll_elements)
            needs_poll = el->setup_poll() or needs_poll;
        return needs_poll;
    }

    /**
     * Perform actions based on poll results.
     *
     * Returns true if the poll loop should stop, false otherwise.
     */
    bool on_poll()
    {
        bool done = false;
        for (auto& el : poll_elements)
            done = el->on_poll(this->result) or done;
        return done;
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
            if (!setup_poll())
                return this->result;

            for (unsigned i = 0; i < 4; ++i)
                this->pollinfo[i].revents = 0;
            int res = Backend::poll(this->pollinfo, 4, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed");
            if (res == 0)
                throw TimedOut("streaming operations timed out");

            trace_streaming("POLL: fi:%d:%x→%x fo:%d:%x→%x fe:%d:%x→%x de:%d:%x→%x\n",
                    pollinfo[0].fd, (int)pollinfo[0].events, (int)pollinfo[0].revents,
                    pollinfo[1].fd, (int)pollinfo[1].events, (int)pollinfo[1].revents,
                    pollinfo[2].fd, (int)pollinfo[2].events, (int)pollinfo[2].revents,
                    pollinfo[3].fd, (int)pollinfo[3].events, (int)pollinfo[3].revents);

            if (on_poll())
                return this->result;
        }
    }
};

/**
 * Send data to an output, through a filter
 */
template<typename Backend, typename Source, typename FromFilter>
struct SenderFiltered : public FilterLoop<Backend, FromFilter>
{
    SenderFiltered(BaseStreamOutput& stream, Source&& source, FromFilter&& from_filter)
        : FilterLoop<Backend, FromFilter>(stream, std::move(from_filter))
    {
        this->add_poll_element(new ToFilter<Backend, Source>(stream, std::move(source)));
    }
};

/**
 * Flush data from a filter after we are done sending to it
 */
template<typename Backend, typename FromFilter>
struct FlushFilter : public FilterLoop<Backend, FromFilter>
{
    using FilterLoop<Backend, FromFilter>::FilterLoop;
};

}
}

#endif
