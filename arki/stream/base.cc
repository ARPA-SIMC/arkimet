#include "base.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include "loops.h"
#include "loops.tcc"
#include "concrete-parts.tcc"
#include <poll.h>
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>
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

    if (filter_process->cmd.get_stdin() != -1)
        filter_process->cmd.close_stdin();

    flush_filter_output();

    std::unique_ptr<FilterProcess> proc = std::move(filter_process);
    proc->stop();

    return std::make_pair(proc->size_stdin, proc->size_stdout);
}

template<template<typename> class ToPipe, typename... Args>
SendResult AbstractStreamOutput::_send_from_pipe(Args&&... args)
{
    SenderFiltered<ConcreteLinuxBackend, ToPipe<ConcreteLinuxBackend>, FromFilterAbstract<ConcreteLinuxBackend>> sender(*this, ToPipe<ConcreteLinuxBackend>(std::forward<Args>(args)...), FromFilterAbstract<ConcreteLinuxBackend>(*this));
    return sender.loop();
}

void AbstractStreamOutput::flush_filter_output()
{
    FlushFilter<ConcreteLinuxBackend, FromFilterAbstract<ConcreteLinuxBackend>> sender(*this, FromFilterAbstract<ConcreteLinuxBackend>(*this));
    sender.loop();
}

stream::SendResult AbstractStreamOutput::_write_output_line(const void* data, size_t size)
{
    stream::SendResult res = _write_output_buffer(data, size);
    res += _write_output_buffer("\n", 1);
    return res;
}

SendResult AbstractStreamOutput::send_buffer(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        return _send_from_pipe<BufferToPipe>(data, size);
    } else {
        if (data_start_callback)
            result += fire_data_start_callback();

        result +=_write_output_buffer(data, size);
    }
    if (progress_callback)
        progress_callback(size);
    return result;
}

SendResult AbstractStreamOutput::send_line(const void* data, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        return _send_from_pipe<LineToPipe>(data, size);
    } else {
        if (data_start_callback)
            result += fire_data_start_callback();

        result += _write_output_line(data, size);
    }
    if (progress_callback)
        progress_callback(size + 1);
    return result;
}

SendResult AbstractStreamOutput::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    SendResult result;
    if (size == 0)
        return result;

    if (filter_process)
    {
        try {
            return _send_from_pipe<FileToPipeSendfile>(fd, offset, size);
        } catch (SendfileNotAvailable&) {
            return _send_from_pipe<FileToPipeReadWrite>(fd, offset, size);
        }
    } else {
        TransferBuffer buffer;
        buffer.allocate();

        size_t pos = 0;
        while (pos < size)
        {
            size_t res = fd.pread(buffer, std::min(buffer.size, size - pos), offset + pos);
            if (res == 0)
                throw std::runtime_error("cannot sendfile() " + std::to_string(offset) + "+" + std::to_string(size) + " to output: the span does not seem to match the file");
            result += send_buffer(buffer, res);
            pos += res;
        }
    }

    return result;
}

ssize_t (*ConcreteLinuxBackend::read)(int fd, void *buf, size_t count) = ::read;
ssize_t (*ConcreteLinuxBackend::write)(int fd, const void *buf, size_t count) = ::write;
ssize_t (*ConcreteLinuxBackend::writev)(int fd, const struct iovec *iov, int iovcnt) = ::writev;
ssize_t (*ConcreteLinuxBackend::sendfile)(int out_fd, int in_fd, off_t *offset, size_t count) = ::sendfile;
ssize_t (*ConcreteLinuxBackend::splice)(int fd_in, loff_t *off_in, int fd_out,
                                                    loff_t *off_out, size_t len, unsigned int flags) = ::splice;
int (*ConcreteLinuxBackend::poll)(struct pollfd *fds, nfds_t nfds, int timeout) = ::poll;
ssize_t (*ConcreteLinuxBackend::pread)(int fd, void *buf, size_t count, off_t offset) = ::pread;

std::function<ssize_t(int fd, void *buf, size_t count)> ConcreteTestingBackend::read = ::read;
std::function<ssize_t(int fd, const void *buf, size_t count)> ConcreteTestingBackend::write = ::write;
std::function<ssize_t(int fd, const struct iovec *iov, int iovcnt)> ConcreteTestingBackend::writev = ::writev;
std::function<ssize_t(int out_fd, int in_fd, off_t *offset, size_t count)> ConcreteTestingBackend::sendfile = ::sendfile;
std::function<ssize_t(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags)> ConcreteTestingBackend::splice = ::splice;
std::function<int(struct pollfd *fds, nfds_t nfds, int timeout)> ConcreteTestingBackend::poll = ::poll;
std::function<ssize_t(int fd, void *buf, size_t count, off_t offset)> ConcreteTestingBackend::pread = ::pread;

void ConcreteTestingBackend::reset()
{
    ConcreteTestingBackend::read = ::read;
    ConcreteTestingBackend::write = ::write;
    ConcreteTestingBackend::writev = ::writev;
    ConcreteTestingBackend::sendfile = ::sendfile;
    ConcreteTestingBackend::splice = ::splice;
    ConcreteTestingBackend::poll = ::poll;
    ConcreteTestingBackend::pread = ::pread;
}

}
}
