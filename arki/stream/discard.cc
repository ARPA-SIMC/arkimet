#include "discard.h"
#include "arki/utils/sys.h"

namespace arki {
namespace stream {

DiscardStreamOutput::DiscardStreamOutput()
{
}

stream::SendResult DiscardStreamOutput::_write_output_buffer(const void* data, size_t size)
{
    return SendResult();
}

stream::SendResult DiscardStreamOutput::_write_output_line(const void* data, size_t size)
{
    return SendResult();
}

}
}
