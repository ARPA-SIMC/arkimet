#ifndef ARKI_STREAM_BUFFER_H
#define ARKI_STREAM_BUFFER_H

#include <arki/core/fwd.h>
#include <arki/stream/abstract.h>
#include <cstdint>
#include <poll.h>

namespace arki {
namespace stream {

class BufferStreamOutput : public AbstractStreamOutput<LinuxBackend>
{
    std::vector<uint8_t>& out;

    stream::SendResult _write_output_buffer(const void* data,
                                            size_t size) override;
    stream::SendResult _write_output_line(const void* data,
                                          size_t size) override;

public:
    BufferStreamOutput(std::vector<uint8_t>& out);

    std::string name() const override { return "memory buffer"; }
    std::filesystem::path path() const override { return "memory buffer"; }
};

} // namespace stream
} // namespace arki

#endif
