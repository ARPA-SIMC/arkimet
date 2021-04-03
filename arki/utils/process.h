#ifndef ARKI_UTILS_PROCESS_H
#define ARKI_UTILS_PROCESS_H

#include <arki/utils/subprocess.h>
#include <string>
#include <vector>
#include <sstream>
#include <signal.h>

namespace arki {
namespace utils {

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

/**
 * Dispatch input and output to and from a child process.
 *
 * This is an abstract base class to use to implement simple select loops. The
 * loop manages only one process, but allows to deal with stdin, stdout and
 * stderr at the same time, without deadlocks.
 */
struct IODispatcher
{
    /// Subprocess that does the filtering
    subprocess::Child& subproc;
    /// Wait timeout: use { 0, 0 } to wait forever (default: { 0, 0 })
    struct timespec conf_timeout;

    IODispatcher(subprocess::Child& subproc);
    virtual ~IODispatcher();

    /// Called when there is data to read on stdout
    virtual void read_stdout() = 0;

    /// Called when there is data to read on stderr
    virtual void read_stderr() = 0;

    /**
     * Send data to the child process
     *
     * This runs a select() loop until all data has been sent. The select loop
     * will call read_stdout() and read_stderr() if data becomes available on
     * the child stdout or stderr.
     *
     * @returns the amount of data that has been sent. This is the same as
     * \a size if all went well, and can be less only if the child process
     * died or closed stdin before all data has been sent.
     */
    size_t send(const void* buf, size_t size);

    /// Shortcut to send a string
    size_t send(const std::string& data)
    {
        return send(data.data(), data.size());
    }

    /// Shortcut to send a buffer
    size_t send(const std::vector<uint8_t>& data)
    {
        return send(data.data(), data.size());
    }

    /**
     * Keep running a select() loop on the child stdout and stderr for as long
     * as they stay open.
     *
     * Please use close_outfd() and close_errfd() to close them when reading
     * from them gives EOF.
     */
    void flush();

    /// Handle activity on the child process' stdout
    void on_stdout(short revents);

    /// Handle activity on the child process' stderr
    void on_stderr(short revents);

    /**
     * Copy data from an input file descriptor to a stream.
     *
     * It performs only one read(), to make sure that this function does not
     * block if it is called after select detected incoming data.
     *
     * @returns false on end of file (EOF), else true
     */
    static bool fd_to_stream(int in, std::ostream& out);

    /**
     * Discard data from an input file descriptor
     *
     * It performs only one read(), to make sure that this function does not
     * block if it is called after select detected incoming data.
     *
     * @returns false on end of file (EOF), else true
     */
    static bool discard_fd(int in);
};

}
}

// vim:set ts=4 sw=4:
#endif
