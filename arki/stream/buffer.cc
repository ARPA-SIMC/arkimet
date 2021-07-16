#include "buffer.h"
#include "filter.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

BufferStreamOutput::BufferStreamOutput(std::vector<uint8_t>& out)
    : out(out)
{
}

stream::SendResult BufferStreamOutput::_write_output_buffer(const void* data, size_t size)
{
    out.insert(out.end(), (const uint8_t*)data, (const uint8_t*)data + size);
    return SendResult();
}

stream::SendResult BufferStreamOutput::_write_output_line(const void* data, size_t size)
{
    out.insert(out.end(), (const uint8_t*)data, (const uint8_t*)data + size);
    out.emplace_back('\n');
    return SendResult();
}

}
}

