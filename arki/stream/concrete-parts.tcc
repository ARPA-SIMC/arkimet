#ifndef ARKI_STREAM_CONCRETE_PARTS_TCC
#define ARKI_STREAM_CONCRETE_PARTS_TCC

#include "arki/stream/fwd.h"
#include "concrete.h"
#include <poll.h>
#include <string>
#include <functional>
#include <system_error>

namespace arki {
namespace stream {

enum class TransferResult
{
    DONE = 0,
    EOF_SOURCE = 1,
    EOF_DEST = 2,
    WOULDBLOCK = 3,
};


/**
 * Base class for event loops that implement the streaming operation
 */
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
    Sender<Backend>* sender_for_data_start_callback = nullptr;
    std::string out_name;
    pollfd& dest;

    ToPipe(const std::string& out_name, pollfd& dest)
        : out_name(out_name), dest(dest)
    {
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
    std::function<void(size_t)> progress_callback;


    MemoryToPipe(const void* data, size_t size, pollfd& dest, const std::string& out_name)
        : ToPipe<Backend>(out_name, dest), data(data), size(size)
    {
    }

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

        ssize_t res = Backend::write(this->dest.fd, (const uint8_t*)this->data + this->pos, this->size - this->pos);
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
            ssize_t res = Backend::writev(this->dest.fd, todo, 2);
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
            ssize_t res = Backend::write(this->dest.fd, "\n", 1);
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

}
}

#endif
