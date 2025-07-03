#ifndef ARKI_STREAM_DISCARD_H
#define ARKI_STREAM_DISCARD_H

#include <arki/stream/abstract.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

class DiscardStreamOutput: public AbstractStreamOutput<LinuxBackend>
{
    stream::SendResult _write_output_buffer(const void* data, size_t size) override;
    stream::SendResult _write_output_line(const void* data, size_t size) override;

public:
    DiscardStreamOutput();

    std::string name() const override { return "discard"; }
    std::filesystem::path path() const override { return "discard"; }
};

}
}

#endif
