#ifndef ARKI_STREAM_LOOPS_TCC
#define ARKI_STREAM_LOOPS_TCC

#include "loops.h"
#include "arki/exceptions.h"
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

template<typename Backend>
stream::SendResult UnfilteredLoop<Backend>::send_buffer(const void* data, size_t size)
{
    return loop(BufferToPipe<Backend>(data, size));
}

template<typename Backend>
stream::SendResult UnfilteredLoop<Backend>::send_line(const void* data, size_t size)
{
    return loop(LineToPipe<Backend>(data, size));
}

template<typename Backend>
stream::SendResult UnfilteredLoop<Backend>::send_file_segment(core::NamedFileDescriptor& src_fd, off_t offset, size_t size)
{
    try {
        return loop(FileToPipeSendfile<Backend>(src_fd, offset, size));
    } catch (SendfileNotAvailable&) {
        return loop(FileToPipeReadWrite<Backend>(src_fd, offset, size));
    }
}

template<typename Backend> template<typename ToOutput>
stream::SendResult UnfilteredLoop<Backend>::loop(ToOutput to_output)
{
    stream::SendResult result;
    while (true)
    {
        pollinfo.revents = 0;
        int res = Backend::poll(&pollinfo, 1, stream.timeout_ms);
        trace_streaming("UnfilteredLoop.POLL: %d %d:%d\n", res, pollinfo.fd, pollinfo.revents);
        if (res < 0)
            throw_system_error(errno, "poll failed on ", stream.out->path());
        if (res == 0)
            throw TimedOut("write on " + stream.out->path().native() + " timed out");
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
            throw_runtime_error("unsupported revents values when polling ", stream.out->path());
    }
}

/**
 * Base for event loops that manage a filter
 */
template<typename Backend, typename FromFilter>
struct FilterLoop : public Sender
{
    BaseStreamOutput& stream;
    stream::SendResult result;
    CollectFilterStderr<Backend> part_connect_stderr;
    /// Move data from the filer stdout to the stream destination
    FromFilter part_from_filter;
    /// pollfd structure described by the POLLINFO_* indices
    pollfd pollinfo[4];

    template<typename StreamOutputClass>
    FilterLoop(StreamOutputClass& stream)
        : stream(stream), part_connect_stderr(stream), part_from_filter(stream)
    {
        for (unsigned i = 0; i < 4; ++i)
        {
            pollinfo[i].fd = -1;
            pollinfo[i].events = 0;
        }

        part_connect_stderr.set_output(pollinfo);
        part_from_filter.set_output(pollinfo);
    }

    virtual ~FilterLoop()
    {
    }

    template<typename Source>
    stream::SendResult send(Source&& source)
    {
        return loop(ToFilter<Backend, Source>(stream, std::move(source)));

    }

    stream::SendResult send_buffer(const void* data, size_t size) override final
    {
        return send(BufferToPipe<Backend>(data, size));
    }

    stream::SendResult send_line(const void* data, size_t size) override final
    {
        return send(LineToPipe<Backend>(data, size));
    }

    stream::SendResult send_file_segment(core::NamedFileDescriptor& src_fd, off_t offset, size_t size) override final
    {
        try {
            return send(FileToPipeSendfile<Backend>(src_fd, offset, size));
        } catch (SendfileNotAvailable&) {
            return send(FileToPipeReadWrite<Backend>(src_fd, offset, size));
        }
    }

    stream::SendResult flush() override final
    {
        return loop(Flusher<Backend>(stream));
    }

    template<typename ToFilter>
    stream::SendResult loop(ToFilter part_to_filter)
    {
        part_to_filter.set_output(pollinfo);

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
            trace_streaming("FilteredLoop.POLL:\n");

            // Setup the pollinfo structure
            bool needs_poll = false;
            needs_poll = part_connect_stderr.setup_poll() or needs_poll;
            needs_poll = part_from_filter.setup_poll() or needs_poll;
            needs_poll = part_to_filter.setup_poll() or needs_poll;
            if (!needs_poll)
            {
                trace_streaming("  FilteredLoop.POLL: stopping after setup_poll returned false\n");
                return this->result;
            }

            for (unsigned i = 0; i < 4; ++i)
                this->pollinfo[i].revents = 0;
            int res = Backend::poll(this->pollinfo, 4, this->stream.timeout_ms);
            if (res < 0)
                throw std::system_error(errno, std::system_category(), "poll failed");
            if (res == 0)
                throw TimedOut("streaming operations timed out");

            trace_streaming("  FilteredLoop.POLL: fi:%d:%x→%x fo:%d:%x→%x fe:%d:%x→%x de:%d:%x→%x\n",
                    pollinfo[0].fd, (int)pollinfo[0].events, (int)pollinfo[0].revents,
                    pollinfo[1].fd, (int)pollinfo[1].events, (int)pollinfo[1].revents,
                    pollinfo[2].fd, (int)pollinfo[2].events, (int)pollinfo[2].revents,
                    pollinfo[3].fd, (int)pollinfo[3].events, (int)pollinfo[3].revents);


            /// Perform actions based on poll results
            bool done = false;
            done = part_connect_stderr.on_poll(this->result) or done;
            done = part_from_filter.on_poll(this->result) or done;
            done = part_to_filter.on_poll(this->result) or done;
            if (done)
            {
                trace_streaming("  FilteredLoop.POLL: stopping after on_poll returned true\n");
                return this->result;
            }
        }
    }
};

}
}

#endif
