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


#if 0
template<typename Backend>
struct NullWriteCallback
{
    NullWriteCallback(ConcreteStreamOutputBase<Backend>& stream) {}
    bool on_write() { return false; }
};


template<typename Backend>
struct DataStartCallback
{
    ConcreteStreamOutputBase<Backend>& stream;

    DataStartCallback(ConcreteStreamOutputBase<Backend>& stream) : stream(stream) {}

    bool on_write()
    {
        if (this->stream.data_start_callback)
        {
            this->result += this->stream.fire_data_start_callback();
            return true;
        }
        return false;
    }
};
#endif


template<typename Backend>
struct BufferToOutput
{
    const void* data;
    size_t size;
    size_t pos = 0;
    pollfd& dest;
    std::string out_name;
    std::function<void(size_t)> progress_callback;

    BufferToOutput(const void* data, size_t size, pollfd& dest, const std::string& out_name)
        : data(data), size(size), dest(dest), out_name(out_name)
    {
    }

    void reset(const void* data, size_t size)
    {
        this->data = data;
        this->size = size;
        pos = 0;
    }

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available()
    {
        ssize_t res = Backend::write(dest.fd, (const uint8_t*)data + pos, size - pos);
        fprintf(stderr, "  BufferToOutput write %.*s %d → %d\n", (int)(size - pos), (const char*)data + pos, (int)(size - pos), (int)res);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(size - pos) + " bytes to " + out_name);
        } else {
            pos += res;

            if (progress_callback)
                progress_callback(res);

            if (pos == (size_t)res)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        }
    }

#if 0
    // Called after each poll() call returns
    bool on_poll()
    {
    }
#endif
};

template<typename Backend>
struct LineToOutput : public BufferToOutput<Backend>
{
    using BufferToOutput<Backend>::BufferToOutput;

    TransferResult transfer_available()
    {
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
