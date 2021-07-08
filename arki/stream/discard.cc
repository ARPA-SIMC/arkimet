#include "discard.h"
#include "arki/utils/sys.h"

namespace arki {
namespace stream {

DiscardStreamOutput::DiscardStreamOutput()
{
}

stream::SendResult DiscardStreamOutput::_write_output_buffer(const void* data, size_t size)
{
    return size;
}

SendResult DiscardStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;

    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    if (progress_callback)
        progress_callback(size + 1);

    result.sent += size + 1;
    return result;
}

}
}
