#include "abstractoutput.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

AbstractOutputStreamOutput::AbstractOutputStreamOutput(core::AbstractOutputFile& out)
    : out(out)
{
}

size_t AbstractOutputStreamOutput::send_line(const void* data, size_t size)
{
    out.write(data, size);
    out.write("\n", 1);
    if (progress_callback)
        progress_callback(size + 1);
    return size + 1;
}

size_t AbstractOutputStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    constexpr size_t buf_size = 4096 * 16;
    char buf[buf_size];
    size_t pos = 0;

    while (pos < size)
    {
        size_t done = fd.pread(buf, std::min(buf_size, size - pos), offset + pos);
        if (done == 0)
            throw std::runtime_error(out.name() + ": read only " + std::to_string(pos) + "/" + std::to_string(size) + " bytes");
        out.write(buf, done);
        pos += done;
        if (progress_callback)
            progress_callback(done);
    }

    return size;
}

size_t AbstractOutputStreamOutput::send_buffer(const void* data, size_t size)
{
    out.write(data, size);
    if (progress_callback)
        progress_callback(size);
    return size;
}

size_t AbstractOutputStreamOutput::send_from_pipe(int fd)
{
    // Read a small chunk of data from child, to trigger data_start_hook
    char buf[4096 * 8];
    ssize_t res = read(fd, buf, 4096 * 8);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "cannot read from pipe");
    if (res == 0)
        return 0;
    out.write(buf, res);
    if (progress_callback)
        progress_callback(res);
    return res;
}

}
}

