#include "base.h"
#include "filter.h"
#include "arki/utils/sys.h"
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

bool BaseStreamOutput::is_nonblocking(int fd)
{
    int src_fl = fcntl(fd, F_GETFL);
    if (src_fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for input");
    return src_fl & O_NONBLOCK;
}

stream::FilterProcess* BaseStreamOutput::start_filter(const std::vector<std::string>& command)
{
    if (filter_process)
        throw std::runtime_error("A filter command was already started on this stream");

    filter_process.reset(new stream::FilterProcess(command));
    filter_process->m_stream = this;
    filter_process->start();
    return filter_process.get();
}

std::unique_ptr<FilterProcess> BaseStreamOutput::stop_filter()
{
    if (!filter_process)
        return std::move(filter_process);

    if (filter_process->cmd.get_stdin() != -1)
        filter_process->cmd.close_stdin();

    flush_filter_output();

    std::unique_ptr<FilterProcess> proc = std::move(filter_process);
    proc->stop();

    return proc;
}

void BaseStreamOutput::abort_filter()
{
    if (!filter_process)
        return;

    std::unique_ptr<FilterProcess> proc = std::move(filter_process);
    proc->stop();
}

Sender::~Sender() {}

ssize_t (*LinuxBackend::read)(int fd, void *buf, size_t count) = ::read;
ssize_t (*LinuxBackend::write)(int fd, const void *buf, size_t count) = ::write;
ssize_t (*LinuxBackend::writev)(int fd, const struct iovec *iov, int iovcnt) = ::writev;
ssize_t (*LinuxBackend::sendfile)(int out_fd, int in_fd, off_t *offset, size_t count) = ::sendfile;
ssize_t (*LinuxBackend::splice)(int fd_in, loff_t *off_in, int fd_out,
                                                    loff_t *off_out, size_t len, unsigned int flags) = ::splice;
int (*LinuxBackend::poll)(struct pollfd *fds, nfds_t nfds, int timeout) = ::poll;
ssize_t (*LinuxBackend::pread)(int fd, void *buf, size_t count, off_t offset) = ::pread;

std::function<ssize_t(int fd, void *buf, size_t count)> TestingBackend::read = ::read;
std::function<ssize_t(int fd, const void *buf, size_t count)> TestingBackend::write = ::write;
std::function<ssize_t(int fd, const struct iovec *iov, int iovcnt)> TestingBackend::writev = ::writev;
std::function<ssize_t(int out_fd, int in_fd, off_t *offset, size_t count)> TestingBackend::sendfile = ::sendfile;
std::function<ssize_t(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags)> TestingBackend::splice = ::splice;
std::function<int(struct pollfd *fds, nfds_t nfds, int timeout)> TestingBackend::poll = ::poll;
std::function<ssize_t(int fd, void *buf, size_t count, off_t offset)> TestingBackend::pread = ::pread;

void TestingBackend::reset()
{
    TestingBackend::read = ::read;
    TestingBackend::write = ::write;
    TestingBackend::writev = ::writev;
    TestingBackend::sendfile = ::sendfile;
    TestingBackend::splice = ::splice;
    TestingBackend::poll = ::poll;
    TestingBackend::pread = ::pread;
}

}
}
