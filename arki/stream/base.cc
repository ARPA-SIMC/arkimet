#include "base.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include <poll.h>
#include <system_error>
#include <unistd.h>
#include <fcntl.h>

namespace arki {
namespace stream {

size_t constexpr TransferBuffer::size;

BaseStreamOutput::BaseStreamOutput() {}
BaseStreamOutput::~BaseStreamOutput() {}

uint32_t BaseStreamOutput::wait_readable(int read_fd)
{
    pollfd pollinfo;
    pollinfo.fd = read_fd;
    pollinfo.events = POLLIN | POLLRDHUP;
    pollinfo.revents = 0;

    // Wait for available input data
    int res = ::poll(&pollinfo, 1, 0);
    if (res < 0)
        throw std::system_error(errno, std::system_category(), "poll failed on input pipe");
    if (res == 0)
        return SendResult::SEND_PIPE_EAGAIN_SOURCE;
    if (pollinfo.revents & POLLRDHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo.revents & POLLERR)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (pollinfo.revents & POLLHUP)
        return SendResult::SEND_PIPE_EOF_SOURCE;
    if (! (pollinfo.revents & POLLIN))
        throw std::runtime_error("unsupported revents values when polling input pipe");

    return 0;
}

bool BaseStreamOutput::is_nonblocking(int fd)
{
    int src_fl = fcntl(fd, F_GETFL);
    if (src_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for input");
    return src_fl & O_NONBLOCK;
}

stream::SendResult BaseStreamOutput::_write_output_line(const void* data, size_t size)
{
    stream::SendResult res = _write_output_buffer(data, size);
    res += _write_output_buffer("\n", 1);
    return res;
}

void BaseStreamOutput::start_filter(const std::vector<std::string>& command)
{
    if (filter_process)
        throw std::runtime_error("A filter command was already started on this stream");

    filter_process.reset(new stream::FilterProcess(command));
    filter_process->m_stream = this;
    filter_process->start();
}

std::pair<size_t, size_t> BaseStreamOutput::stop_filter()
{
    if (!filter_process)
        return std::make_pair(0u, 0u);

    std::unique_ptr<FilterProcess> proc = std::move(filter_process);

    proc->stop();

    return std::make_pair(proc->size_stdin, proc->size_stdout);
}

SendResult BaseStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        // Leave data_start_callback to send_from_pipe, so we trigger it only
        // if/when data is generated
        filter_process->send(data, size);
        filter_process->size_stdin += size;
    } else {
        if (data_start_callback)
            result += fire_data_start_callback();

        result +=_write_output_buffer(data, size);
    }
    if (progress_callback)
        progress_callback(size);
    return result;
}

SendResult BaseStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        // Leave data_start_callback to send_from_pipe, so we trigger it only
        // if/when data is generated
        filter_process->send(data, size);
        filter_process->send("\n");
        filter_process->size_stdin += size + 1;
    } else {
        if (data_start_callback)
            result += fire_data_start_callback();

        result += _write_output_line(data, size);
    }
    if (progress_callback)
        progress_callback(size + 1);
    return result;
}

SendResult BaseStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
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

std::pair<size_t, SendResult> BaseStreamOutput::send_from_pipe(int fd)
{
    size_t sent = 0;
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

        if (data_start_callback)
            result += fire_data_start_callback();

        result += _write_output_buffer(buffer, res);
        sent += res;
        if (progress_callback)
            progress_callback(res);
    }

    return std::make_pair(sent, result);
}


}
}
