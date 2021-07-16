#ifndef ARKI_STREAM_LOOPS_SEND_TCC
#define ARKI_STREAM_LOOPS_SEND_TCC

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

/**
 * Base class for functions that write data to pipes
 */
template<typename Backend>
struct ToPipe
{
    std::function<void(size_t)> progress_callback;

    ToPipe() = default;
    ToPipe(const ToPipe&) = default;
    ToPipe(ToPipe&&) = default;
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
    TransferResult transfer_available(core::NamedFileDescriptor& out)
    {
        ssize_t res = Backend::write(out, (const uint8_t*)this->data + this->pos, this->size - this->pos);
        trace_streaming("  BufferToPipe[pos:%zd]: Write(%d, \"%.*s\", %d, %d) [errno %d]\n",
                this->pos, (int)out, (int)(this->size - this->pos), (const char*)this->data + this->pos, (int)(this->size - this->pos), (int)res, errno);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size - this->pos) + " bytes to " + out.name());
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

    TransferResult transfer_available(core::NamedFileDescriptor& out)
    {
        if (this->pos < this->size)
        {
            struct iovec todo[2] = {{(uint8_t*)this->data + this->pos, this->size - this->pos}, {(void*)"\n", 1}};
            ssize_t res = Backend::writev(out, todo, 2);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return TransferResult::WOULDBLOCK;
                else if (errno == EPIPE)
                    return TransferResult::EOF_DEST;
                else
                    throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size + 1) + " bytes to " + out.name());
            }
            if (this->progress_callback)
                this->progress_callback(res);
            this->pos += res;
            if (this->pos == this->size + 1)
                return TransferResult::DONE;
            else
                return TransferResult::WOULDBLOCK;
        } else if (this->pos == this->size) {
            ssize_t res = Backend::write(out, "\n", 1);
            if (res < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return TransferResult::WOULDBLOCK;
                else if (errno == EPIPE)
                    return TransferResult::EOF_DEST;
                else
                    throw std::system_error(errno, std::system_category(), "cannot write 1 byte to " + out.name());
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
    TransferResult transfer_available(core::NamedFileDescriptor& out)
    {
#ifdef TRACE_STREAMING
        auto orig_offset = offset;
#endif
        ssize_t res = Backend::sendfile(out, src_fd, &offset, size - pos);
        trace_streaming("  FileToPipeSendfile sendfile fd: %d→%d offset: %d→%d size: %d → %d\n",
                 (int)src_fd, (int)out, (int)orig_offset, (int)offset, (int)(size - pos), (int)res);
        if (res < 0)
        {
            if (errno == EINVAL || errno == ENOSYS)
                throw SendfileNotAvailable();
            else if (errno == EPIPE)
                return TransferResult::EOF_DEST;
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else
                throw std::system_error(errno, std::system_category(), "cannot sendfile() " + std::to_string(size) + " bytes to " + out.name());
        } else if (res == 0)
            throw std::runtime_error("cannot sendfile() " + std::to_string(offset) + "+" + std::to_string(size) + " to " + out.name() + ": the span does not seem to match the file");
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
    std::array<uint8_t, 4096 * 8> buffer;

    FileToPipeReadWrite(core::NamedFileDescriptor& src_fd, off_t offset, size_t size)
        : ToPipe<Backend>(), src_fd(src_fd), offset(offset), size(size)
    {
    }
    FileToPipeReadWrite(const FileToPipeReadWrite&) = delete;
    FileToPipeReadWrite(FileToPipeReadWrite&&) = default;

    /**
     * Called when poll signals that we can write to the destination
     */
    TransferResult transfer_available(core::NamedFileDescriptor& out)
    {
        if (write_pos >= write_size)
        {
            ssize_t res = Backend::pread(src_fd, buffer.data(), std::min(size - pos, buffer.size()), offset);
            if (res == -1)
                src_fd.throw_error("cannot pread");
            else if (res == 0)
                return TransferResult::EOF_SOURCE;
            write_size = res;
            write_pos = 0;
            offset += res;
        }

        ssize_t res = Backend::write(out, buffer.data() + write_pos, write_size - write_pos);
        trace_streaming("  BufferToOutput write %.*s %d → %d\n", (int)(write_size - write_pos), (const char*)buffer.data() + write_pos, (int)(write_size - write_pos), (int)res);
        if (res < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return TransferResult::WOULDBLOCK;
            else if (errno == EPIPE) {
                return TransferResult::EOF_DEST;
            } else
                throw std::system_error(errno, std::system_category(), "cannot write " + std::to_string(this->size - this->pos) + " bytes to " + out.name());
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

}
}

#endif
