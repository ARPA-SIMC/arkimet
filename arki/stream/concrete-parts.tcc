#ifndef ARKI_STREAM_CONCRETE_PARTS_TCC
#define ARKI_STREAM_CONCRETE_PARTS_TCC

#include "arki/stream/fwd.h"
#include "arki/utils/sys.h"
#include "filter.h"
#include "loops.h"
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

#if 0
    // Called after each poll() call returns
    bool on_poll()
    {
    }
#endif
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
        fprintf(stderr, "  BufferToOutput write %.*s %d → %d\n", (int)(this->size - this->pos), (const char*)this->data + this->pos, (int)(this->size - this->pos), (int)res);
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

            if (this->pos == (size_t)res)
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
            struct iovec todo[2] = {{(void*)this->data, this->size}, {(void*)"\n", 1}};
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

        ssize_t res = Backend::sendfile(this->dest->fd, src_fd, &offset, size - pos);
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
        // fprintf(stderr, "  BufferToOutput write %.*s %d → %d\n", (int)(this->size - this->pos), (const char*)this->data + this->pos, (int)(this->size - this->pos), (int)res);
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

template<typename Backend>
struct FromFilter
{
    BaseStreamOutput& stream;
    Sender* sender_for_data_start_callback = nullptr;
    pollfd* pfd_filter;
    std::function<void(size_t)> progress_callback;
    std::string out_name;
    bool filter_stdout_available = false;

    FromFilter(BaseStreamOutput& stream) : stream(stream) {}
    FromFilter(const FromFilter&) = default;
    FromFilter(FromFilter&&) = default;

    void set_output(pollfd& pfd_filter)
    {
        this->pfd_filter = &pfd_filter;
        this->pfd_filter->fd = stream.filter_process->cmd.get_stdout();
        this->pfd_filter->events = POLLIN;
    }

    void setup_poll()
    {
        if (filter_stdout_available)
            pfd_filter->fd = -1;
        else
            pfd_filter->fd = stream.filter_process->cmd.get_stdout();
    }

    void set_available_flags()
    {
        if (pfd_filter->revents & POLLIN)
            filter_stdout_available = true;
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
    pollfd* pfd_out;
    bool destination_available = false;
    FromFilterConcrete(ConcreteStreamOutputBase<Backend>& stream) : FromFilter<Backend>(stream), out_fd(*stream.out) {}
    FromFilterConcrete(const FromFilterConcrete&) = default;
    FromFilterConcrete(FromFilterConcrete&&) = default;

    void set_output(pollfd& pfd_filter, pollfd& pfd_out)
    {
        FromFilter<Backend>::set_output(pfd_filter);
        this->out_name = out_fd.name();
        this->pfd_out = &pfd_out;
        this->pfd_out->fd = out_fd;
        this->pfd_out->events = POLLOUT;
    }

    void setup_poll()
    {
        FromFilter<Backend>::setup_poll();
        if (destination_available)
            pfd_out->fd = -1;
        else
            pfd_out->fd = out_fd;
    }

    void set_available_flags()
    {
        FromFilter<Backend>::set_available_flags();

        if (this->pfd_out->revents & POLLOUT)
            destination_available = true;
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
        while (true)
        {
            utils::Sigignore ignpipe(SIGPIPE);
            // Try splice
            ssize_t res = Backend::splice(this->stream.filter_process->cmd.get_stdout(), NULL, this->out_fd, NULL, TransferBuffer::size * 128, SPLICE_F_MORE | SPLICE_F_NONBLOCK);
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

    bool on_poll(SendResult& result)
    {
        this->set_available_flags();
        fprintf(stderr, "  FromFilterSplice.on_poll %d %d\n", this->filter_stdout_available, this->destination_available);

        if (this->filter_stdout_available and this->destination_available)
        {
            this->filter_stdout_available = false;
            this->destination_available = false;

            switch (this->transfer_available_output())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // Filter closed stdout, evidently we are done
                    return true;
                case TransferResult::EOF_DEST:
                    // Destination closed its pipe
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    return true;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }
        return false;
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

    void set_output(pollfd& pfd_filter, pollfd& pfd_out)
    {
        FromFilterConcrete<Backend>::set_output(pfd_filter, pfd_out);

        ConcreteStreamOutputBase<Backend>* stream = reinterpret_cast<ConcreteStreamOutputBase<Backend>*>(&(this->stream));
        to_output.set_output(*stream->out, pfd_out);
    }

    TransferResult transfer_available_output_read()
    {
        to_output.reset(buffer.buf, 0);
        ssize_t res = Backend::read(this->stream.filter_process->cmd.get_stdout(), buffer, buffer.size);
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

    bool on_poll(SendResult& result)
    {
        fprintf(stderr, "  FromFilterReadWrite.on_poll\n");
        this->set_available_flags();

        // TODO: this could append to the output buffer to chunk together small writes
        if (to_output.size == 0 && this->filter_stdout_available)
        {
            this->filter_stdout_available = false;
            switch (transfer_available_output_read())
            {
                case TransferResult::DONE:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
                case TransferResult::EOF_SOURCE:
                    // TODO: Filter closed stdout, stop polling it
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
                    result.flags |= SendResult::SEND_PIPE_EOF_DEST;
                    return true;
                case TransferResult::WOULDBLOCK:
                    break;
            }
        }
        return false;
    }
};


template<typename Backend>
struct FromFilterAbstract : public FromFilter<Backend>
{
    TransferBuffer buffer;

    FromFilterAbstract(AbstractStreamOutput& stream) : FromFilter<Backend>(stream) { buffer.allocate(); }
    FromFilterAbstract(const FromFilterAbstract&) = default;
    FromFilterAbstract(FromFilterAbstract&&) = default;

    void set_output(pollfd& pfd_filter, pollfd& pfd_out)
    {
        FromFilter<Backend>::set_output(pfd_filter);
        this->out_name = "output";
    }

    TransferResult transfer_available_output()
    {
        ssize_t res = Backend::read(this->pfd_filter->fd, buffer, buffer.size);
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
            AbstractStreamOutput* stream = reinterpret_cast<AbstractStreamOutput*>(&(this->stream));
            this->check_data_start_callback();
            stream->_write_output_buffer(buffer.buf, res);
            return TransferResult::WOULDBLOCK;
        }
    }

    bool on_poll(SendResult& result)
    {
        this->set_available_flags();

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
                    return true;
                case TransferResult::EOF_DEST:
                    throw std::runtime_error("unexpected result from feed_filter_stdin");
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
