#include "stream.h"
#include "arki/stream/concrete.h"
#include "arki/stream/concrete-timeout.h"
#include "arki/stream/abstractoutput.h"
#include <ostream>

using namespace arki::utils;

namespace arki {

StreamOutput::~StreamOutput()
{
}

std::unique_ptr<StreamOutput> StreamOutput::create(core::NamedFileDescriptor& out)
{
    return std::unique_ptr<StreamOutput>(new stream::ConcreteStreamOutput(out));
}

std::unique_ptr<StreamOutput> StreamOutput::create(core::NamedFileDescriptor& out, unsigned timeout_ms)
{
    return std::unique_ptr<StreamOutput>(new stream::ConcreteTimeoutStreamOutput(out, timeout_ms));
}

std::unique_ptr<StreamOutput> StreamOutput::create(core::AbstractOutputFile& out)
{
    return std::unique_ptr<StreamOutput>(new stream::AbstractOutputStreamOutput(out));
}


namespace stream {

constexpr uint32_t SendResult::SEND_PIPE_EOF_SOURCE;
constexpr uint32_t SendResult::SEND_PIPE_EOF_DEST;
constexpr uint32_t SendResult::SEND_PIPE_EAGAIN_SOURCE;

std::ostream& operator<<(std::ostream& out, const SendResult& r)
{
    if (r.flags == 0)
        return out << "[" << r.sent << "]";
    else
    {
        out << "[" << r.sent << ", ";
        if (r.flags & SendResult::SEND_PIPE_EOF_SOURCE)
            out << "S";
        else
            out << "-";
        if (r.flags & SendResult::SEND_PIPE_EOF_DEST)
            out << "D";
        else
            out << "-";
        if (r.flags & SendResult::SEND_PIPE_EAGAIN_SOURCE)
            out << "s";
        else
            out << "-";
        return out << "]";
    }
}

}

}
