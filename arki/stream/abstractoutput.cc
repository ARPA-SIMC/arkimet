#include "abstractoutput.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

AbstractOutputStreamOutput::AbstractOutputStreamOutput(std::shared_ptr<core::AbstractOutputFile> out)
    : out(out)
{
}

std::string AbstractOutputStreamOutput::name() const { return out->name(); }

SendResult AbstractOutputStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out->write(data, size);
    if (progress_callback)
        progress_callback(size);
    result.sent += size;

    return result;
}

SendResult AbstractOutputStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;

    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out->write(data, size);
    out->write("\n", 1);
    if (progress_callback)
        progress_callback(size + 1);

    result.sent += size + 1;
    return result;
}

}
}
