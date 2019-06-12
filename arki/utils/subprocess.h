#ifndef ARKI_UTILS_SUBPROCESS_H
#define ARKI_UTILS_SUBPROCESS_H

#include <vector>
#include <string>
#include <sys/types.h>

namespace arki {
namespace utils {
namespace subprocess {

enum class Redirect
{
    /** Redirect the file descriptor to a pipe. The variabile will be set to the
     * parent end of the pipe after fork.
     */
    PIPE,

    /// Redirect a file descriptor to /dev/null
    DEVNULL,

    /// Redirect stderr to stdout
    STDOUT,

    /// Redirect to a file descriptor
    FD,

    /// No change
    UNCHANGED,
};


class Child
{
protected:
    pid_t m_pid = 0;
    int m_returncode = 0;
    bool m_terminated = false;
    int m_stdin[2] = { -1, -1 };
    int m_stdout[2] = { -1, -1 };
    int m_stderr[2] = { -1, -1 };
    Redirect m_stdin_action = Redirect::UNCHANGED;
    Redirect m_stdout_action = Redirect::UNCHANGED;
    Redirect m_stderr_action = Redirect::UNCHANGED;

    /// Function called before forking
    virtual void pre_fork();

    /// Function called after fork in the parent process
    virtual void post_fork_parent();

    /// Function called after fork in the child process
    virtual void post_fork_child();

    /**
     * Main function called in the child process.
     *
     * Its result will be used as the return code for the process.
     */
    virtual int main() noexcept = 0;

public:
    /// After fork, close all file descriptors >=2 in the child
    bool close_fds = true;

    /**
     * Do not close these file descriptors in the child process
     * (implies close_fds = true)
     */
    std::vector<int> pass_fds;

    /// Change to this directory in the child process
    std::string cwd;

    /// If true, call setsid() in the child process
    bool start_new_session = false;

    /// Return the file descriptor to the stdin pipe to the child process, if configured, else thrown an exception
    int stdin() const;
    /// Return the file descriptor to the stdout pipe from the child process, if configured, else thrown an exception
    int stdout() const;
    /// Return the file descriptor to the stderr pipe from the child process, if configured, else thrown an exception
    int stderr() const;

    /// Request to redirect the child stdin to this given file descriptor
    void stdin(int fd);
    /// Request to redirect the child stdin according to val
    void stdin(Redirect val);
    /// Request to redirect the child stdout to this given file descriptor
    void stdout(int fd);
    /// Request to redirect the child stdout according to val
    void stdout(Redirect val);
    /// Request to redirect the child stderr to this given file descriptor
    void stderr(int fd);
    /// Request to redirect the child stderr according to val
    void stderr(Redirect val);

    virtual ~Child();

    /// Start the child process
    void fork();

    /// Return the PID of the subprocess, or 0 if it has not started yet
    pid_t pid() const { return m_pid; }

    /**
     * Return the return code of the subprocess; this is undefined if it has
     * not terminated yet.
     */
    int returncode() const;

    /// Return true if the process has started
    bool started() const { return m_pid != 0; }

    /// Return true if the process has terminated
    bool terminated() const { return m_terminated; }

    /// Check if the process has terminated. Returns true if it has.
    bool poll();

    // Wait for the child process to terminate
    void wait();

    // Send the given signal to the process
    void send_signal(int sig);

    // Send SIGTERM to the process
    void terminate();

    // Send SIGKILL to the process
    void kill();
};


class Popen : public Child
{
protected:
    int main() noexcept override;

public:
    std::vector<std::string> args;
    std::string executable;

    // env=None
};


}
}
}

#endif
