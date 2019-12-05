#include "arki/libconfig.h"
#include "process.h"
#include "arki/exceptions.h"
#include <poll.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>

#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;

namespace {

/*
 * One can ignore SIGPIPE (using, for example, the signal system call). In this
 * case, all system calls that would cause SIGPIPE to be sent will return -1
 * and set errno to EPIPE.
 */
struct Sigignore
{
    int signum;
    sighandler_t oldsig;

    Sigignore(int signum) : signum(signum)
    {
        oldsig = signal(signum, SIG_IGN);
    }
    ~Sigignore()
    {
        signal(signum, oldsig);
    }
};

}

namespace arki {
namespace utils {

IODispatcher::IODispatcher(subprocess::Child& subproc)
    : subproc(subproc)
{
    conf_timeout.tv_sec = 0;
    conf_timeout.tv_nsec = 0;
}

IODispatcher::~IODispatcher()
{
}

bool IODispatcher::fd_to_stream(int in, std::ostream& out)
{
    // Read data from child
    char buf[4096*2];
    ssize_t res = read(in, buf, 4096*2);
    if (res < 0)
        throw_system_error("reading from child stderr");
    if (res == 0)
        return false;

    // Pass it on
    out.write(buf, res);
    if (out.bad())
        throw_system_error("writing to destination stream");
    return true;
}

bool IODispatcher::discard_fd(int in)
{
    // Read data from child
    char buf[4096*2];
    ssize_t res = read(in, buf, 4096*2);
    if (res < 0)
        throw_system_error("reading from child stderr");
    if (res == 0)
        return false;
    return true;
}

size_t IODispatcher::send(const void* buf, size_t size)
{
    struct timespec* timeout_param = NULL;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    struct pollfd fds[3];
    int stdin_idx, stdout_idx, stderr_idx;

    size_t written = 0;
    while (written < size && subproc.get_stdin() != -1)
    {
        int nfds = 0;

        if (subproc.get_stdin() != -1)
        {
            stdin_idx = nfds;
            fds[nfds].fd = subproc.get_stdin();
            fds[nfds].events = POLLOUT;
            ++nfds;
        } else
            stdin_idx = -1;

        if (subproc.get_stdout() != -1)
        {
            stdout_idx = nfds;
            fds[nfds].fd = subproc.get_stdout();
            fds[nfds].events = POLLIN;
            ++nfds;
        } else
            stdout_idx = -1;

        if (subproc.get_stderr() != -1)
        {
            stderr_idx = nfds;
            fds[nfds].fd = subproc.get_stderr();
            fds[nfds].events = POLLIN;
            ++nfds;
        } else
            stderr_idx = -1;

        if (nfds == 0) break;

        int res = ppoll(fds, nfds, timeout_param, nullptr);
        if (res < 0)
            throw_system_error("cannot wait for activity on filter child process");
        if (res == 0)
            throw std::runtime_error("timeout waiting for activity on filter child process");

        if (stdin_idx != -1)
        {
            if (fds[stdin_idx].revents & POLLOUT)
            {
                // Ignore SIGPIPE so we get EPIPE
                Sigignore ignpipe(SIGPIPE);
                ssize_t res = write(subproc.get_stdin(), (unsigned char*)buf + written, size - written);
                if (res < 0)
                {
                    if (errno == EPIPE)
                        subproc.close_stdin();
                    else
                        throw_system_error("cannot write to child process");
                } else {
                    written += res;
                }
            }
            else if (fds[stdin_idx].revents & POLLHUP)
                subproc.close_stdin();
        }

        if (stdout_idx != -1)
        {
            if (fds[stdout_idx].revents & POLLIN)
                read_stdout();
            else if (fds[stdout_idx].revents & POLLHUP)
                subproc.close_stdout();
        }

        if (stderr_idx != -1)
        {
            if (fds[stderr_idx].revents & POLLIN)
                read_stderr();
            else if (fds[stderr_idx].revents & POLLHUP)
                subproc.close_stderr();
        }
    }
    return written;
}

void IODispatcher::flush()
{
    if (subproc.get_stdin() != -1)
        subproc.close_stdin();

    struct timespec* timeout_param = nullptr;
    if (conf_timeout.tv_sec != 0 || conf_timeout.tv_nsec != 0)
        timeout_param = &conf_timeout;

    struct pollfd fds[2];
    int stdout_idx, stderr_idx;

    while (true)
    {
        int nfds = 0;

        if (subproc.get_stdout() != -1)
        {
            stdout_idx = nfds;
            fds[nfds].fd = subproc.get_stdout();
            fds[nfds].events = POLLIN;
            ++nfds;
        } else
            stdout_idx = -1;

        if (subproc.get_stderr() != -1)
        {
            stderr_idx = nfds;
            fds[nfds].fd = subproc.get_stderr();
            fds[nfds].events = POLLIN;
            ++nfds;
        } else
            stderr_idx = -1;

        if (nfds == 0) break;

        int res = ppoll(fds, nfds, timeout_param, nullptr);
        if (res < 0)
            throw_system_error("cannot wait for activity on filter child process");
        if (res == 0)
            throw std::runtime_error("timeout waiting for activity on filter child process");

        if (stdout_idx != -1)
        {
            if (fds[stdout_idx].revents & POLLIN)
                read_stdout();
            else if (fds[stdout_idx].revents & POLLHUP)
                subproc.close_stdout();
        }

        if (stderr_idx != -1)
        {
            if (fds[stderr_idx].revents & POLLIN)
                read_stderr();
            else if (fds[stderr_idx].revents & POLLHUP)
                subproc.close_stderr();
        }
    }
}

}
}
