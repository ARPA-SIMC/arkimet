#include "stream.h"
#include "arki/stream/buffer.h"
#include "arki/stream/concrete.h"
#include "arki/stream/discard.h"
#include <ostream>

using namespace arki::utils;

namespace arki {

StreamOutput::~StreamOutput() {}

std::unique_ptr<StreamOutput>
StreamOutput::create(std::shared_ptr<core::NamedFileDescriptor> out,
                     unsigned timeout_ms)
{
    if (timeout_ms > 0)
        return std::unique_ptr<StreamOutput>(
            new stream::ConcreteStreamOutput(out, timeout_ms));
    else
        return std::unique_ptr<StreamOutput>(
            new stream::ConcreteStreamOutput(out));
}

std::unique_ptr<StreamOutput> StreamOutput::create(std::vector<uint8_t>& out)
{
    return std::unique_ptr<StreamOutput>(new stream::BufferStreamOutput(out));
}

std::unique_ptr<StreamOutput> StreamOutput::create_discard()
{
    return std::unique_ptr<StreamOutput>(new stream::DiscardStreamOutput());
}

namespace stream {

constexpr uint32_t SendResult::SEND_PIPE_EOF_SOURCE;
constexpr uint32_t SendResult::SEND_PIPE_EOF_DEST;

std::ostream& operator<<(std::ostream& out, const SendResult& r)
{
    if (r.flags == 0)
        return out << "[--]";
    else
    {
        out << "[";
        if (r.flags & SendResult::SEND_PIPE_EOF_SOURCE)
            out << "S";
        else
            out << "-";
        if (r.flags & SendResult::SEND_PIPE_EOF_DEST)
            out << "D";
        else
            out << "-";
        return out << "]";
    }
}

} // namespace stream

} // namespace arki
