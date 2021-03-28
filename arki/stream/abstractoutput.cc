#include "abstractoutput.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

AbstractOutputStreamOutput::AbstractOutputStreamOutput(core::AbstractOutputFile& out)
    : out(out)
{
}

size_t AbstractOutputStreamOutput::send_buffer(const void* data, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    if (data_start_callback) sent += fire_data_start_callback();

    out.write(data, size);
    if (progress_callback)
        progress_callback(size);
    sent += size;

    return sent;
}

size_t AbstractOutputStreamOutput::send_line(const void* data, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    if (data_start_callback) sent += fire_data_start_callback();

    out.write(data, size);
    out.write("\n", 1);
    if (progress_callback)
        progress_callback(size + 1);

    sent += size + 1;
    return sent;
}

size_t AbstractOutputStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    size_t sent = 0;
    if (size == 0)
        return sent;

    TransferBuffer buffer;
    buffer.allocate();

    size_t pos = 0;
    while (pos < size)
    {
        size_t res = fd.pread(buffer, std::min(buffer.size, size - pos), offset + pos);
        if (res == 0)
            break;
        sent += send_buffer(buffer, res);
        pos += res;
    }

    return sent;
}

size_t AbstractOutputStreamOutput::send_from_pipe(int fd)
{
    size_t sent = 0;

    TransferBuffer buffer;
    buffer.allocate();
    while (true)
    {
        ssize_t res = read(fd, buffer, buffer.size);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "cannot read from pipe");
        if (res == 0)
            break;

        sent += send_buffer(buffer, res);
    }

    return sent;
}

}
}

