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

    /// Return the file descriptor to the stdin pipe to the child process, if configured, else -1
    int get_stdin() const;
    /// Return the file descriptor to the stdout pipe from the child process, if configured, else -1
    int get_stdout() const;
    /// Return the file descriptor to the stderr pipe from the child process, if configured, else -1
    int get_stderr() const;

    /// Request to redirect the child stdin to this given file descriptor
    void set_stdin(int fd);
    /// Request to redirect the child stdin according to val
    void set_stdin(Redirect val);
    /// Request to redirect the child stdout to this given file descriptor
    void set_stdout(int fd);
    /// Request to redirect the child stdout according to val
    void set_stdout(Redirect val);
    /// Request to redirect the child stderr to this given file descriptor
    void set_stderr(int fd);
    /// Request to redirect the child stderr according to val
    void set_stderr(Redirect val);

    /// Close the pipe to the child process stdin
    void close_stdin();
    /// Close the pipe from the child process stdout
    void close_stdout();
    /// Close the pipe from the child process stderr
    void close_stderr();

    Child() = default;
    Child(const Child&) = delete;
    Child(Child&&) = delete;
    virtual ~Child();

    Child& operator=(const Child&) = delete;
    Child& operator=(Child&&) = delete;

    /// Start the child process
    void fork();

    /// Return the PID of the subprocess, or 0 if it has not started yet
    pid_t pid() const { return m_pid; }

    /**
     * Return the return code of the subprocess; this is undefined if it has
     * not terminated yet.
     */
    int returncode() const;

    /// Return the raw return code as returned by wait(2)
    int raw_returncode() const { return m_returncode; }

    /// Return true if the process has started
    bool started() const { return m_pid != 0; }

    /// Return true if the process has terminated
    bool terminated() const { return m_terminated; }

    /// Check if the process has terminated. Returns true if it has.
    bool poll();

    /// Wait for the child process to terminate and return its return code
    int wait();

    /// Send the given signal to the process
    void send_signal(int sig);

    /// Send SIGTERM to the process
    void terminate();

    /// Send SIGKILL to the process
    void kill();

    /// Format the status code returned by wait(2)
    static std::string format_raw_returncode(int raw_returncode);
};


class Popen : public Child
{
protected:
    int main() noexcept override;

public:
    /// argv of the child process
    std::vector<std::string> args;
    /// pathname to the executable of the child process, defaults to args[0] if empty
    std::string executable;
    /// environment variables to use for the child process
    std::vector<std::string> env;

    using Child::Child;

    Popen(std::initializer_list<std::string> args);

    /// Override env with the contents of environment
    void copy_env_from_parent();

    void setenv(const std::string& key, const std::string& val);
};


}
}
}

#endif
