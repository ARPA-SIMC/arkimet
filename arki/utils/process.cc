#include "process.h"
#include "arki/exceptions.h"
#include <poll.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
#include <stdexcept>

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

        if (stdin_idx != -1 && fds[stdin_idx].revents)
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
            else if (fds[stdin_idx].revents & POLLERR)
            {
                Sigignore ignpipe(SIGPIPE);
                ssize_t res = write(subproc.get_stdin(), (unsigned char*)buf, 0);
                if (res < 0)
                {
                    if (errno == EPIPE)
                        subproc.close_stdin();
                    else
                        throw_system_error("cannot write to child process");
                } else
                    // POLLERR is also returned "for a file descriptor
                    // referring to the write end of a pipe when the read end
                    // has been closed."
                    subproc.close_stdin();
            }
            else
                fprintf(stderr, "Unexpected poll results on stdin: %d %s %s %s %s %s %s %s\n",
                        (int)fds[stdin_idx].revents,
                        (fds[stdin_idx].revents & POLLIN) ? "pollin" : "-",
                        (fds[stdin_idx].revents & POLLPRI) ? "pollpri" : "-",
                        (fds[stdin_idx].revents & POLLOUT) ? "pollout" : "-",
                        (fds[stdin_idx].revents & POLLRDHUP) ? "pollrdhup" : "-",
                        (fds[stdin_idx].revents & POLLERR) ? "pollerr" : "-",
                        (fds[stdin_idx].revents & POLLHUP) ? "pollhup" : "-",
                        (fds[stdin_idx].revents & POLLNVAL) ? "pollnval" : "-");
        }

        if (stdout_idx != -1 && fds[stdout_idx].revents)
        {
            on_stdout(fds[stdout_idx].revents);
        }

        if (stderr_idx != -1 && fds[stderr_idx].revents)
        {
            on_stderr(fds[stderr_idx].revents);
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

        if (stdout_idx != -1 && fds[stdout_idx].revents)
            on_stdout(fds[stdout_idx].revents);

        if (stderr_idx != -1 && fds[stderr_idx].revents)
            on_stderr(fds[stderr_idx].revents);
    }
}

void IODispatcher::on_stdout(short revents)
{
    if (revents & POLLIN)
        read_stdout();
    else if (revents & POLLHUP)
        subproc.close_stdout();
    else if (revents & POLLERR)
    {
        char buf[1];
        ssize_t res = ::read(subproc.get_stdout(), (unsigned char*)buf, 0);
        if (res < 0)
        {
            if (errno == EPIPE)
                subproc.close_stdout();
            else
                throw_system_error("cannot read stdout from child process");
        }
    }
    else
        fprintf(stderr, "Unexpected poll results on stdout: %d %s %s %s %s %s %s %s\n",
                (int)revents,
                (revents & POLLIN) ? "pollin" : "-",
                (revents & POLLPRI) ? "pollpri" : "-",
                (revents & POLLOUT) ? "pollout" : "-",
                (revents & POLLRDHUP) ? "pollrdhup" : "-",
                (revents & POLLERR) ? "pollerr" : "-",
                (revents & POLLHUP) ? "pollhup" : "-",
                (revents & POLLNVAL) ? "pollnval" : "-");
}

void IODispatcher::on_stderr(short revents)
{
    if (revents & POLLIN)
        read_stderr();
    else if (revents & POLLHUP)
        subproc.close_stderr();
    else if (revents & POLLERR)
    {
        char buf[1];
        ssize_t res = ::read(subproc.get_stderr(), (unsigned char*)buf, 0);
        if (res < 0)
        {
            if (errno == EPIPE)
                subproc.close_stderr();
            else
                throw_system_error("cannot read stderr from child process");
        }
    }
    else
        fprintf(stderr, "Unexpected poll results on stderr: %d %s %s %s %s %s %s %s\n",
                (int)revents,
                (revents & POLLIN) ? "pollin" : "-",
                (revents & POLLPRI) ? "pollpri" : "-",
                (revents & POLLOUT) ? "pollout" : "-",
                (revents & POLLRDHUP) ? "pollrdhup" : "-",
                (revents & POLLERR) ? "pollerr" : "-",
                (revents & POLLHUP) ? "pollhup" : "-",
                (revents & POLLNVAL) ? "pollnval" : "-");
}

}
}
