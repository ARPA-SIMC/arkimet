#ifndef ARKI_CORE_STREAM_H
#define ARKI_CORE_STREAM_H

#include <arki/core/fwd.h>
#include <vector>
#include <memory>
#include <cstdint>
#include <sys/types.h>

namespace arki {
namespace core {


class StreamOutput
{
public:
    virtual ~StreamOutput();

    virtual size_t send_vm2_line(const std::vector<uint8_t>& line) = 0;
    virtual size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) = 0;
    virtual size_t send_buffer(const void* buf, size_t size) = 0;
    virtual size_t send_from_pipe(int fd) = 0;

    static std::unique_ptr<StreamOutput> create(NamedFileDescriptor& out);
    static std::unique_ptr<StreamOutput> create(AbstractOutputFile& out);
};


}
}
#endif
