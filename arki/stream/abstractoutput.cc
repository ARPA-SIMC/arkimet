#include "abstractoutput.h"
#include "arki/core/file.h"

namespace arki {
namespace stream {

AbstractOutputStreamOutput::AbstractOutputStreamOutput(core::AbstractOutputFile& out)
    : out(out)
{
}

SendResult AbstractOutputStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out.write(data, size);
    if (progress_callback)
        progress_callback(size);
    result.sent += size;

    return result;
}

SendResult AbstractOutputStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;

    if (size == 0)
        return result;

    if (data_start_callback)
        result += fire_data_start_callback();

    out.write(data, size);
    out.write("\n", 1);
    if (progress_callback)
        progress_callback(size + 1);

    result.sent += size + 1;
    return result;
}

SendResult AbstractOutputStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    TransferBuffer buffer;
    buffer.allocate();

    size_t pos = 0;
    while (pos < size)
    {
        size_t res = fd.pread(buffer, std::min(buffer.size, size - pos), offset + pos);
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            break;
        }
        result += send_buffer(buffer, res);
        pos += res;
    }

    return result;
}

SendResult AbstractOutputStreamOutput::send_from_pipe(int fd)
{
    SendResult result;

    TransferBuffer buffer;
    buffer.allocate();
    while (true)
    {
        ssize_t res = read(fd, buffer, buffer.size);
        if (res < 0)
        {
            if (errno == EAGAIN) {
                result.flags |= SendResult::SEND_PIPE_EAGAIN_SOURCE;
                break;
            } else
                throw std::system_error(errno, std::system_category(), "cannot read from pipe");
        }
        if (res == 0)
        {
            result.flags |= SendResult::SEND_PIPE_EOF_SOURCE;
            break;
        }

        result += send_buffer(buffer, res);
    }

    return result;
}

}
}

