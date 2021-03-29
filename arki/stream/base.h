#ifndef ARKI_STREAM_BASE_H
#define ARKI_STREAM_BASE_H

#include <arki/stream.h>

namespace arki {
namespace stream {

class BaseStreamOutput : public StreamOutput
{
protected:
    std::function<void(size_t)> progress_callback;
    std::function<stream::SendResult(StreamOutput&)> data_start_callback;

    /**
     * Fire data_start_callback then set it to nullptr
     *
     * It disarms the callback before firing it, to prevent firing it
     * recursively if it calls other send_ operations on the stream.
     */
    stream::SendResult fire_data_start_callback()
    {
        auto cb = data_start_callback;
        data_start_callback = nullptr;
        return cb(*this);
    }

    /**
     * Returns:
     *  0 if the source has data available
     *  an OR combination of SendResult flags if a known condition happened
     *  that should interrupt the writing
     */
    uint32_t wait_readable(int read_fd);

    /**
     * Check if a file descriptor is in nonblocking mode
     */
    bool is_nonblocking(int fd);

public:
    void set_progress_callback(std::function<void(size_t)> f) override
    {
        progress_callback = f;
    }

    void set_data_start_callback(std::function<stream::SendResult(StreamOutput&)> f) override
    {
        data_start_callback = f;
    }
};

class BaseConcreteStreamOutput : public BaseStreamOutput
{
protected:
    std::shared_ptr<core::NamedFileDescriptor> out;

public:
    BaseConcreteStreamOutput(std::shared_ptr<core::NamedFileDescriptor> out)
        : out(out)
    {
    }

    std::string name() const override;
};

struct TransferBuffer
{
    constexpr static size_t size = 4096 * 8;
    char* buf = nullptr;

    TransferBuffer() = default;
    TransferBuffer(const TransferBuffer&) = delete;
    TransferBuffer(TransferBuffer&&) = delete;
    ~TransferBuffer()
    {
        delete[] buf;
    }
    TransferBuffer& operator=(const TransferBuffer&) = delete;
    TransferBuffer& operator=(TransferBuffer&&) = delete;

    void allocate()
    {
        if (buf)
            return;
        buf = new char[size];
    }

    operator char*() { return buf; }
};

}
}

#endif
