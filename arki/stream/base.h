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

public:
    void set_progress_callback(std::function<void(size_t)> f) override
    {
        progress_callback = f;
    }

    void set_data_start_callback(std::function<void(StreamOutput&)> f) override
    {
    }
};

}
}

#endif
