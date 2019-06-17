#include "subprocess.h"
#include "sys.h"
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sysexits.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
#include <system_error>
#include <algorithm>

namespace arki {
namespace utils {
namespace subprocess {

Child::~Child()
{
    switch (m_stdin_action)
    {
        case Redirect::PIPE:
            if (m_stdin[1] != -1) ::close(m_stdin[1]);
            break;
        default:
            break;
    }

    switch (m_stdout_action)
    {
        case Redirect::PIPE:
            if (m_stdout[0] != -1) ::close(m_stdout[0]);
            break;
        default:
            break;
    }

    switch (m_stderr_action)
    {
        case Redirect::PIPE:
            if (m_stderr[0] != -1) ::close(m_stderr[0]);
            break;
        default:
            break;
    }
}

int Child::returncode() const
{
    return WEXITSTATUS(m_returncode);
}

int Child::get_stdin() const
{
    return m_stdin[1];
}

int Child::get_stdout() const
{
    return m_stdout[0];
}

int Child::get_stderr() const
{
    return m_stderr[0];
}

void Child::set_stdin(int fd)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stdin after the child process has started");
    m_stdin[0] = fd;
    m_stdin[1] = -1;
    m_stdin_action = Redirect::FD;
}

void Child::set_stdout(int fd)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stdout after the child process has started");
    m_stdout[0] = -1;
    m_stdout[1] = fd;
    m_stdout_action = Redirect::FD;
}

void Child::set_stderr(int fd)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stderr after the child process has started");
    m_stderr[0] = -1;
    m_stderr[1] = fd;
    m_stderr_action = Redirect::FD;
}

void Child::set_stdin(Redirect val)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stdin after the child process has started");
    m_stdin_action = val;
}

void Child::set_stdout(Redirect val)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stdin after the child process has started");
    m_stdout_action = val;
}

void Child::set_stderr(Redirect val)
{
    if (m_pid != 0)
        throw std::runtime_error("cannot redirect stderr after the child process has started");
    m_stderr_action = val;
}

void Child::close_stdin()
{
    close(m_stdin[1]);
    m_stdin[1] = -1;
}

void Child::close_stdout()
{
    close(m_stdout[0]);
    m_stdout[0] = -1;
}

void Child::close_stderr()
{
    close(m_stderr[0]);
    m_stderr[0] = -1;
}

void Child::pre_fork()
{
    switch (m_stdin_action)
    {
        case Redirect::PIPE:
            if (pipe(m_stdin) == -1)
                throw std::system_error(errno, std::system_category(), "failed to create a pipe for stdin");
            break;
        default:
            break;
    }

    switch (m_stdout_action)
    {
        case Redirect::PIPE:
            if (pipe(m_stdout) == -1)
                throw std::system_error(errno, std::system_category(), "failed to create a pipe for stdout");
            break;
        default:
            break;
    }

    switch (m_stderr_action)
    {
        case Redirect::PIPE:
            if (pipe(m_stderr) == -1)
                throw std::system_error(errno, std::system_category(), "failed to create a pipe for stderr");
            break;
        default:
            break;
    }
}

void Child::post_fork_parent()
{
    switch (m_stdin_action)
    {
        case Redirect::PIPE:
            if (close(m_stdin[0]) == -1)
                throw std::system_error(errno, std::system_category(), "failed close read end of stdin pipe in parent process");
            m_stdin[0] = -1;
            break;
        default:
            break;
    }

    switch (m_stdout_action)
    {
        case Redirect::PIPE:
            if (close(m_stdout[1]) == -1)
                throw std::system_error(errno, std::system_category(), "failed close read end of stdout pipe in parent process");
            m_stdout[1] = -1;
            break;
        default:
            break;
    }

    switch (m_stderr_action)
    {
        case Redirect::PIPE:
            if (close(m_stderr[1]) == -1)
                throw std::system_error(errno, std::system_category(), "failed close read end of stderr pipe in parent process");
            m_stderr[1] = -1;
            break;
        default:
            break;
    }
}

static void redirect(int newfd, int oldfd, const char* errmsg)
{
    if (dup2(newfd, oldfd) == -1)
        throw std::system_error(errno, std::system_category(), errmsg);
    if (close(newfd) == -1 )
        throw std::system_error(errno, std::system_category(), errmsg);
}

void Child::post_fork_child()
{
    {
        sys::File fd_devnull("/dev/null");

        switch (m_stdin_action)
        {
            case Redirect::FD:
                redirect(m_stdin[0], STDIN_FILENO, "failed to redirect stdin");
                break;
            case Redirect::PIPE:
                if (close(m_stdin[1]) == -1)
                    throw std::system_error(errno, std::system_category(), "failed close write end of stdin pipe in child process");
                redirect(m_stdin[0], STDIN_FILENO, "failed to redirect stdin");
                break;
            case Redirect::DEVNULL:
                if (fd_devnull == -1)
                    fd_devnull.open(O_RDWR);
                if (dup2(fd_devnull, STDIN_FILENO) == -1)
                    throw std::system_error(errno, std::system_category(), "failed to redirect stdin to /dev/null");
                break;
            default:
                break;
        }

        switch (m_stdout_action)
        {
            case Redirect::FD:
                redirect(m_stdout[1], STDOUT_FILENO, "failed to redirect stdout");
                break;
            case Redirect::PIPE:
                if (close(m_stdout[0]) == -1)
                    throw std::system_error(errno, std::system_category(), "failed close read end of stdout pipe in child process");
                redirect(m_stdout[1], STDOUT_FILENO, "failed to redirect stdout");
                break;
            case Redirect::DEVNULL:
                if (fd_devnull == -1)
                    fd_devnull.open(O_RDWR);
                if (dup2(fd_devnull, STDOUT_FILENO) == -1)
                    throw std::system_error(errno, std::system_category(), "failed to redirect stdout to /dev/null");
                break;
            default:
                break;
        }

        switch (m_stderr_action)
        {
            case Redirect::FD:
                redirect(m_stderr[1], STDERR_FILENO, "failed to redirect stderr");
                break;
            case Redirect::PIPE:
                if (close(m_stderr[0]) == -1)
                    throw std::system_error(errno, std::system_category(), "failed close read end of stderr pipe in child process");
                redirect(m_stderr[1], STDERR_FILENO, "failed to redirect stderr");
                break;
            case Redirect::DEVNULL:
                if (fd_devnull == -1)
                    fd_devnull.open(O_RDWR);
                if (dup2(fd_devnull, STDERR_FILENO) == -1)
                    throw std::system_error(errno, std::system_category(), "failed to redirect stderr to /dev/null");
                break;
            case Redirect::STDOUT:
                if (dup2(STDOUT_FILENO, STDERR_FILENO) == -1)
                    throw std::system_error(errno, std::system_category(), "failed to redirect stderr to stdout");
            default:
                break;
        }
    }

    // Close parent file descriptors if requested
    if (close_fds)
    {
        errno = 0;
        long max = sysconf(_SC_OPEN_MAX);
        if (errno == 0)
        {
            for (long i = 3; i < max; ++i)
            {
                if (std::find(pass_fds.begin(), pass_fds.end(), i) != pass_fds.end())
                    continue;
                ::close(i);
            }
        }
        errno = 0;
    }

    // Honor cwd
    if (!cwd.empty())
    {
        if (chdir(cwd.c_str()) == -1)
            throw std::system_error(errno, std::system_category(), "failed to chdir to " + cwd);
    }

    // Honor start_new_session
    if (start_new_session)
        if (setsid() == (pid_t)-1)
            throw std::system_error(errno, std::system_category(), "cannot call setsid()");
}

void Child::fork()
{
    if (!pass_fds.empty())
        close_fds = true;

    pre_fork();

    pid_t pid;
    if ((pid = ::fork()) == 0)
    {
        try {
            post_fork_child();
        } catch (std::system_error& e) {
            fprintf(::stderr, "Child process setup failed: %s\n", e.what());
            _exit(EX_OSERR);
        } catch (std::exception& e) {
            fprintf(::stderr, "Child process setup failed: %s\n", e.what());
            _exit(EX_SOFTWARE);
        }
        _exit(main());
    } else if (pid < 0) {
        throw std::system_error(errno, std::system_category(), "failed to fork()");
    } else {
        m_pid = pid;
        post_fork_parent();
    }
}


bool Child::poll()
{
    if (m_pid == 0)
        throw std::runtime_error("poll called before Child process was started");

    if (m_terminated)
        return true;

    pid_t res = waitpid(m_pid, &m_returncode, WNOHANG);
    if (res == -1)
        throw std::system_error(
                errno, std::system_category(),
                "failed to waitpid(" + std::to_string(m_pid) + ")");

    if (res == m_pid)
    {
        m_terminated = true;
        return true;
    }

    return false;
}

int Child::wait()
{
    if (m_pid == 0)
        throw std::runtime_error("wait called before Child process was started");

    if (m_terminated)
        return returncode();

    pid_t res = waitpid(m_pid, &m_returncode, 0);
    if (res == -1)
        throw std::system_error(
                errno, std::system_category(),
                "failed to waitpid(" + std::to_string(m_pid) + ")");

    m_terminated = true;
    return returncode();
}

void Child::send_signal(int sig)
{
    if (::kill(m_pid, sig) == -1)
        throw std::system_error(
                errno, std::system_category(),
                "cannot send signal " + std::to_string(sig) + " to child PID " + std::to_string(m_pid));
}

void Child::terminate() { send_signal(SIGTERM); }

void Child::kill() { send_signal(SIGKILL); }


Popen::Popen(std::initializer_list<std::string> args)
    : args(args)
{
}

int Popen::main() noexcept
{
    try {
        const char* path;
        if (executable.empty())
            path = args[0].c_str();
        else
            path = executable.c_str();

        const char** exec_args = new const char*[args.size() + 1];
        for (unsigned i = 0; i < args.size(); ++i)
            exec_args[i] = args[i].c_str();
        exec_args[args.size()] = nullptr;

        if (execvp(path, (char* const*)exec_args) == -1)
        {
            delete[] exec_args;
            throw std::system_error(
                    errno, std::system_category(),
                    "execvp failed");
        }

        delete[] exec_args;
        throw std::runtime_error("process flow continued after execvp did not fail");
    } catch (std::system_error& e) {
        fprintf(::stderr, "Child process setup failed: %s\n", e.what());
        return EX_OSERR;
    } catch (std::exception& e) {
        fprintf(::stderr, "Child process setup failed: %s\n", e.what());
        return EX_SOFTWARE;
    }
}

}
}
}
