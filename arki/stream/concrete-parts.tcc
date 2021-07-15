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

namespace arki {
namespace stream {

template<class Sender>
struct NullWriteCallback
{
    NullWriteCallback(Sender& sender) {}
    bool on_write() { return false; }
};


template<class Sender>
struct DataStartCallback
{
    Sender& sender;

    DataStartCallback(Sender& sender) : sender(sender) {}

    bool on_write()
    {
        if (sender.stream.data_start_callback)
        {
            sender.result += sender.stream.fire_data_start_callback();
            return true;
        }
        return false;
    }
};


/**
 * Base class for functions that write data to pipes
 */
template<typename Backend>
struct ToPipe
{
    Sender* sender_for_data_start_callback = nullptr;
    std::string out_name;
    pollfd* dest;
    std::function<void(size_t)> progress_callback;

    ToPipe() = default;
    ToPipe(const ToPipe&) = default;
    ToPipe(ToPipe&&) = default;

    void set_output(core::NamedFileDescriptor out, pollfd& dest)
    {
        this->out_name = out.name();
        this->dest = &dest;
    }

    /**
     * Check if we should trigger data_start_callback on the first write, and
     * if so, trigger it.
     *
     * Returns true if triggered, false otherwise.
     */
    bool check_data_start_callback()
    {
        if (auto sender = sender_for_data_start_callback)
        {
            if (sender->stream.data_start_callback)
            {
                sender->result += sender->stream.fire_data_start_callback();
                return true;
            }
        }
        return false;
    }
};


template<typename Backend>
struct MemoryToPipe : public ToPipe<Backend>
{
    const void* data;
    size_t size;
    size_t pos = 0;

    MemoryToPipe(const void* data, size_t size)
        : ToPipe<Backend>(), data(data), size(size)
    {
    }
    MemoryToPipe(const MemoryToPipe&) = default;
    MemoryToPipe(MemoryToPipe&&) = default;


    void reset(const void* data, size_t size)
    {
        this->data = data;
        this->size = size;
        pos = 0;
    }
};


template<typename Backend>
struct BufferToPipe : public MemoryToPipe<Backend>
{
    using MemoryToPipe<Backend>::MemoryToPipe;

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available()
    {
        if (this->check_data_start_callback())
            return TransferResult::WOULDBLOCK;

        ssize_t res = Backend::write(this->dest->fd, (const uint8_t*)this->data + this->pos, this->size - this->pos);
        trace_streaming("  BufferToPipe write pos:%zd %.*s %d → %d\n", this->pos, (int)(this->size - this->pos), (const char*)this->data + this->pos, (int)(this->size - this->pos), (int)res);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size - this->pos) + " bytes to " + this->out_name);
        } else {
            this->pos += res;

            if (this->progress_callback)
                this->progress_callback(res);

            if (this->pos == this->size)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        }
    }
};

template<typename Backend>
struct LineToPipe : public MemoryToPipe<Backend>
{
    using MemoryToPipe<Backend>::MemoryToPipe;

    TransferResult transfer_available()
    {
        if (this->check_data_start_callback())
            return TransferResult::WOULDBLOCK;

        if (this->pos < this->size)
        {
            struct iovec todo[2] = {{(uint8_t*)this->data + this->pos, this->size - this->pos}, {(void*)"\n", 1}};
            ssize_t res = Backend::writev(this->dest->fd, todo, 2);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return TransferResult::WOULDBLOCK;
                else if (errno == EPIPE)
                    return TransferResult::EOF_DEST;
                else
                    throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size + 1) + " bytes to " + this->out_name);
            }
            if (this->progress_callback)
                this->progress_callback(res);
            this->pos += res;
            if (this->pos == this->size + 1)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        } else if (this->pos == this->size) {
            ssize_t res = Backend::write(this->dest->fd, "\n", 1);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return TransferResult::WOULDBLOCK;
                else if (errno == EPIPE)
                    return TransferResult::EOF_DEST;
                else
                    throw std::system_error(errno, std::system_category(), "cannot write 1 byte to " + this->out_name);
            } else if (res == 0) {
                return TransferResult::WOULDBLOCK;
            } else {
                if (this->progress_callback)
                    this->progress_callback(res);
                this->pos += res;
                return TransferResult::DONE;
            }
        } else
            return TransferResult::DONE;
    }
};

template<typename Backend>
struct FileToPipeSendfile : public ToPipe<Backend>
{
    core::NamedFileDescriptor& src_fd;
    off_t offset;
    size_t size;
    size_t pos = 0;

    FileToPipeSendfile(core::NamedFileDescriptor& src_fd, off_t offset, size_t size)
        : ToPipe<Backend>(), src_fd(src_fd), offset(offset), size(size)
    {
    }
    FileToPipeSendfile(const FileToPipeSendfile&) = delete;
    FileToPipeSendfile(FileToPipeSendfile&&) = default;

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available()
    {
        if (this->check_data_start_callback())
            return TransferResult::WOULDBLOCK;

#ifdef TRACE_STREAMING
        auto orig_offset = offset;
#endif
        ssize_t res = Backend::sendfile(this->dest->fd, src_fd, &offset, size - pos);
        trace_streaming("  FileToPipeSendfile sendfile fd: %d→%d offset: %d→%d size: %d → %d\n",
                 (int)src_fd, this->dest->fd, (int)orig_offset, (int)offset, (int)(size - pos), (int)res);
        if (res < 0)
        {
            if (errno == EINVAL || errno == ENOSYS)
                throw SendfileNotAvailable();
            else if (errno == EPIPE)
                return TransferResult::EOF_DEST;
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else
                throw std::system_error(errno, std::system_category(), "cannot sendfile() " + std::to_string(size) + " bytes to " + this->out_name);
        } else if (res == 0)
            throw std::runtime_error("cannot sendfile() " + std::to_string(offset) + "+" + std::to_string(size) + " to " + this->out_name + ": the span does not seem to match the file");
        else {
            if (this->progress_callback)
                this->progress_callback(res);

            pos += res;

            if (pos == size)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        }
    }
};

template<typename Backend>
struct FileToPipeReadWrite : public ToPipe<Backend>
{
    core::NamedFileDescriptor& src_fd;
    off_t offset;
    size_t size;
    size_t pos = 0;
    size_t write_size = 0;
    size_t write_pos = 0;
    TransferBuffer buffer;

    FileToPipeReadWrite(core::NamedFileDescriptor& src_fd, off_t offset, size_t size)
        : ToPipe<Backend>(), src_fd(src_fd), offset(offset), size(size)
    {
        buffer.allocate();
    }
    FileToPipeReadWrite(const FileToPipeReadWrite&) = delete;
    FileToPipeReadWrite(FileToPipeReadWrite&&) = default;

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available()
    {
        if (this->check_data_start_callback())
            return TransferResult::WOULDBLOCK;

        if (write_pos >= write_size)
        {
            ssize_t res = Backend::pread(src_fd, buffer, std::min(size - pos, buffer.size), offset);
            if (res == -1)
                src_fd.throw_error("cannot pread");
            else if (res == 0)
                return TransferResult::EOF_SOURCE;
            write_size = res;
            write_pos = 0;
            offset += res;
        }

        ssize_t res = Backend::write(this->dest->fd, buffer + write_pos, write_size - write_pos);
        trace_streaming("  BufferToOutput write %.*s %d → %d\n", (int)(write_size - write_pos), (const char*)buffer + write_pos, (int)(write_size - write_pos), (int)res);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size - this->pos) + " bytes to " + this->out_name);
        } else {
            pos += res;
            write_pos += res;

            if (this->progress_callback)
                this->progress_callback(res);

            if (pos == size)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        }
    }
};


/**
 * Event loop addon that collects stderr from the filter process into the
 * `errors` buffer on the FilterProcess object
 */
template<typename Backend>
struct CollectFilterStderr : public PollElement
{
    BaseStreamOutput& stream;
    pollfd* pfd_filter_stderr;
    TransferBuffer stderr_buffer;

    CollectFilterStderr(BaseStreamOutput& stream) : stream(stream) { stderr_buffer.allocate(); }

    void set_output(pollfd* pollinfo) override
    {
        pfd_filter_stderr = &pollinfo[POLLINFO_FILTER_STDERR];
        pfd_filter_stderr->fd = stream.filter_process->cmd.get_stderr();
        pfd_filter_stderr->events = POLLIN;
    }

    void transfer_available_stderr()
    {
        ssize_t res = Backend::read(stream.filter_process->cmd.get_stderr(), stderr_buffer, stderr_buffer.size);
        trace_streaming("  read stderr → %d %.*s\n", (int)res, (int)res, (const char*)stderr_buffer);
        if (res == 0)
        {
            stream.filter_process->cmd.close_stderr();
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
            stream.filter_process->errors.write(stderr_buffer, res);
            if (stream.filter_process->errors.bad())
                throw std::system_error(errno, std::system_category(), "cannot store filter stderr in memory buffer");
        }
    }

    bool setup_poll() override
    {
        return stream.filter_process->cmd.get_stderr() != -1;
    }

    bool on_poll(SendResult& result) override
    {
        if (pfd_filter_stderr->revents & POLLIN)
            transfer_available_stderr();

        if (pfd_filter_stderr->revents & (POLLERR | POLLHUP))
        {
            // Filter stderr closed its endpoint
            stream.filter_process->cmd.close_stderr();
            pfd_filter_stderr->fd = -1;
        }

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
struct ToFilter : public PollElement
{
    BaseStreamOutput& stream;
    pollfd* pfd_filter_stdin;
    ToPipe to_pipe;

    ToFilter(BaseStreamOutput& stream, ToPipe&& to_pipe)
        : stream(stream), to_pipe(std::move(to_pipe))
    {
    }

    void set_output(pollfd* pollinfo) override
    {
        pfd_filter_stdin = &pollinfo[POLLINFO_FILTER_STDIN];
        pfd_filter_stdin->fd = stream.filter_process->cmd.get_stdin();
        pfd_filter_stdin->events = POLLOUT;
        to_pipe.set_output(core::NamedFileDescriptor(stream.filter_process->cmd.get_stdin(), "filter stdin"), pollinfo[POLLINFO_FILTER_STDIN]);
    }

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult feed_filter_stdin()
    {
        auto pre = to_pipe.pos;
        auto res = to_pipe.transfer_available();
        this->stream.filter_process->size_stdin += to_pipe.pos - pre;
        return res;
    }

    bool setup_poll() override
    {
        return stream.filter_process->cmd.get_stdin() != -1;
    }

    bool on_poll(SendResult& result) override
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
            // TODO: child process closed stdin but we were still writing on it
            result.flags |= SendResult::SEND_PIPE_EOF_DEST; // This is not the right return code
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
struct FromFilter : public PollElement
{
    BaseStreamOutput& stream;
    Sender* sender_for_data_start_callback = nullptr;
    pollfd* pfd_filter_stdout;
    std::function<void(size_t)> progress_callback;
    std::string out_name;
    bool filter_stdout_available = false;

    FromFilter(BaseStreamOutput& stream) : stream(stream) {}
    FromFilter(const FromFilter&) = default;
    FromFilter(FromFilter&&) = default;

    void set_output(pollfd* pollinfo) override
    {
        pfd_filter_stdout = &pollinfo[POLLINFO_FILTER_STDOUT];
        pfd_filter_stdout->fd = stream.filter_process->cmd.get_stdout();
        pfd_filter_stdout->events = POLLIN;
    }

    bool setup_poll() override
    {
        if (filter_stdout_available)
            pfd_filter_stdout->events = 0;
        else
            pfd_filter_stdout->events = POLLIN;

        trace_streaming("  FromFilter.setup_poll: filter_stdout: %d:%x\n", stream.filter_process->cmd.get_stdout(), (int)pfd_filter_stdout->events);
        return stream.filter_process->cmd.get_stdout() != -1;
    }

    bool on_poll(SendResult& result) override
    {
        if (pfd_filter_stdout->revents & POLLIN)
            filter_stdout_available = true;

        return false;
    }

    /**
     * Check if we should trigger data_start_callback on the first write, and
     * if so, trigger it.
     *
     * Returns true if triggered, false otherwise.
     */
    bool check_data_start_callback()
    {
        if (auto sender = sender_for_data_start_callback)
        {
            if (sender->stream.data_start_callback)
            {
                sender->result += sender->stream.fire_data_start_callback();
                return true;
            }
        }
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

    void set_output(pollfd* pollinfo) override
    {
        FromFilter<Backend>::set_output(pollinfo);
        this->out_name = out_fd.name();
        this->pfd_destination = &pollinfo[POLLINFO_DESTINATION];
        this->pfd_destination->fd = out_fd;
        this->pfd_destination->events = POLLOUT;
    }

    bool setup_poll() override
    {
        bool res = FromFilter<Backend>::setup_poll();

        if (destination_available)
            pfd_destination->events = 0;
        else
            pfd_destination->events = POLLOUT;

        return res;
    }

    bool on_poll(SendResult& result) override
    {
        bool done = FromFilter<Backend>::on_poll(result);

        if (this->pfd_destination->revents & POLLOUT)
            destination_available = true;

        if (this->pfd_destination->revents & (POLLERR | POLLHUP))
        {
            // Destination closed its endpoint, stop here
            // TODO: stop writing to filter stdin and drain filter stdout?
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
        if (this->check_data_start_callback())
            return TransferResult::WOULDBLOCK;

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

    bool on_poll(SendResult& result) override
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

    FromFilterReadWrite(ConcreteStreamOutputBase<Backend>& stream)
        : FromFilterConcrete<Backend>(stream), to_output(nullptr, 0)
    {
        buffer.allocate();
    }
    FromFilterReadWrite(const FromFilterReadWrite&) = default;
    FromFilterReadWrite(FromFilterReadWrite&&) = default;

    void set_output(pollfd* pollinfo) override
    {
        FromFilterConcrete<Backend>::set_output(pollinfo);

        ConcreteStreamOutputBase<Backend>* stream = reinterpret_cast<ConcreteStreamOutputBase<Backend>*>(&(this->stream));
        to_output.set_output(*stream->out, pollinfo[POLLINFO_DESTINATION]);
    }

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
            this->check_data_start_callback();
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

    bool setup_poll() override
    {
        bool res = FromFilter<Backend>::setup_poll();
        res = res or (to_output.size > 0);
        return res;
    }

    bool on_poll(SendResult& result) override
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

    void set_output(pollfd* pollinfo) override
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
            this->check_data_start_callback();
            stream->_write_output_buffer(buffer.buf, res);
            this->stream.filter_process->size_stdout += res;
            return TransferResult::WOULDBLOCK;
        }
    }

    bool on_poll(SendResult& result) override
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
                    // TODO: Filter closed stdout, stop polling it
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
