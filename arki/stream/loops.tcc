#ifndef ARKI_STREAM_LOOPS_TCC
#define ARKI_STREAM_LOOPS_TCC

#include "loops.h"
#include "filter.h"
#include "concrete.h"
#include "concrete-parts.tcc"

namespace arki {
namespace stream {

template<typename Backend>
UnfilteredLoop<Backend>::UnfilteredLoop(ConcreteStreamOutputBase<Backend>& stream)
    : stream(stream)
{
    pollinfo.fd = *stream.out;
    pollinfo.events = POLLOUT;
}

template<typename Backend> template<typename ToOutput>
stream::SendResult UnfilteredLoop<Backend>::loop(ToOutput to_output)
{
    stream::SendResult result;
    while (true)
    {
        pollinfo.revents = 0;
        int res = Backend::poll(&pollinfo, 1, stream.timeout_ms);
        trace_streaming("SenderDirect.POLL: %d %d:%d\n", res, pollinfo.fd, pollinfo.revents);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "poll failed on " + stream.out->name());
        if (res == 0)
            throw TimedOut("write on " + stream.out->name() + " timed out");
        if (pollinfo.revents & (POLLERR | POLLHUP))
            return SendResult::SEND_PIPE_EOF_DEST;
        if (pollinfo.revents & POLLOUT)
        {
            switch (to_output.transfer_available(*stream.out))
            {
                case TransferResult::DONE:
                    return result;
                case TransferResult::EOF_SOURCE:
                    result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
                    return result;
                case TransferResult::EOF_DEST:
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    return result;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }
        else
            throw std::runtime_error("unsupported revents values when polling " + stream.out->name());
    }
}

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
            {
                trace_streaming("POLL: stopping after setup_poll returned false\n");
                return this->result;
            }

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
            {
                trace_streaming("POLL: stopping after on_poll returned true\n");
                return this->result;
            }
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
