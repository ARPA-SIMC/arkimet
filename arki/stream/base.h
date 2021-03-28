#ifndef ARKI_STREAM_BASE_H
#define ARKI_STREAM_BASE_H

#include <arki/stream.h>

namespace arki {
namespace stream {

class BaseStreamOutput : public StreamOutput
{
protected:
    std::function<void(size_t)> progress_callback;
    std::function<void(StreamOutput&)> data_start_callback;

    /**
     * Fire data_start_callback then set it to nullptr
     *
     * It disarms the callback before firing it, to prevent firing it
     * recursively if it calls other send_ operations on the stream.
     */
    void fire_data_start_callback()
    {
        auto cb = data_start_callback;
        data_start_callback = nullptr;
        cb(*this);
    }

public:
    void set_progress_callback(std::function<void(size_t)> f) override
    {
        progress_callback = f;
    }

    void set_data_start_callback(std::function<void(StreamOutput&)> f) override
    {
        data_start_callback = f;
    }
};

}
}

#endif
