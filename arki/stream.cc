#include "stream.h"
#include "arki/stream/concrete.h"
#include "arki/stream/concrete-timeout.h"
#include "arki/stream/abstractoutput.h"

using namespace arki::utils;

namespace arki {
StreamOutput::~StreamOutput()
{
}

namespace stream {

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

}
