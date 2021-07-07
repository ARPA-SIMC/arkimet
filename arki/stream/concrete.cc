#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

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

template class ConcreteStreamOutputBase<ConcreteLinuxBackend>;
template class ConcreteStreamOutputBase<ConcreteTestingBackend>;

}
}

#include "concrete.tcc"
