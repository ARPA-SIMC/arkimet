#include "buffer.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

BufferStreamOutput::BufferStreamOutput(std::vector<uint8_t>& out)
    : out(out)
{
}

SendResult BufferStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out.insert(out.end(), (const uint8_t*)data, (const uint8_t*)data + size);
    if (progress_callback)
        progress_callback(size);
    result.sent += size;

    return result;
}

SendResult BufferStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;

    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out.insert(out.end(), (const uint8_t*)data, (const uint8_t*)data + size);
    out.emplace_back('\n');
    if (progress_callback)
        progress_callback(size + 1);

    result.sent += size + 1;
    return result;
}

}
}

