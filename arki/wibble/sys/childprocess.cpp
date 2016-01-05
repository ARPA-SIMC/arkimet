#include "childprocess.h"
#include "arki/exceptions.h"
#include <stdlib.h>		// EXIT_FAILURE
#include <sys/types.h>		// fork, waitpid, kill, open, getpw*, getgr*, initgroups
#include <sys/stat.h>		// open
#include <unistd.h>			// fork, dup2, pipe, close, setsid, _exit, chdir
#include <fcntl.h>			// open
#include <sys/resource.h>	// getrlimit, setrlimit
#include <sys/wait.h>		// waitpid
#include <signal.h>			// kill
#include <pwd.h>			// getpw*
#include <grp.h>			// getgr*, initgroups
#include <stdio.h>			// flockfile, funlockfile
#include <ctype.h>			// is*
#include <errno.h>
#include <sstream>

using namespace arki;
using namespace std;

namespace wibble {
namespace sys {

void ChildProcess::spawnChild() {
}

pid_t ChildProcess::fork()
{
    flockfile(stdin);
    flockfile(stdout);
    flockfile(stderr);

    setupPrefork();

    pid_t pid;
    if ((pid = ::fork()) == 0)
    {
        // Tell the logging system we're in a new process
        //Log::Logger::instance()->setupForkedChild();

        // Child process
        try {
            setupChild();

            // no need to funlockfile here, since the library resets the stream
            // locks in the child after a fork

            // Call the process main function and return the exit status
            _exit(main());
        } catch (std::exception& e) {
            //log_err(string(e.type()) + ": " + e.desc());
        }
        _exit(EXIT_FAILURE);

    } else if (pid < 0) {

        funlockfile(stdin);
        funlockfile(stderr);
        funlockfile(stdout);
        throw_system_error("trying to fork a child process");

    } else {
        funlockfile(stdin);
        funlockfile(stderr);
        funlockfile(stdout);

        _pid = pid;
        setupParent();

        // Parent process
        return _pid;
    }
}

void mkpipe( int *fds, int *infd, int *outfd, const char *err )
{
    if (pipe(fds) == -1)
        throw_system_error( err );
    if (infd)
        *infd = fds[0];
    if (outfd)
        *outfd = fds[1];
}

void renamefd( int _old, int _new, const char *err = "..." )
{
    if ( dup2( _old, _new ) == -1 )
        throw_system_error( err );
    if ( close( _old ) == -1 )
        throw_system_error( err );
}

void ChildProcess::setupRedirects(int* _stdinfd, int* _stdoutfd, int* _stderrfd) {
    _stdin = _stdinfd;
    _stdout = _stdoutfd;
    _stderr = _stderrfd;

    if (_stdin)
        mkpipe( pipes[0], 0, _stdin,
                "trying to create the pipe to connect to child standard input" );

    if (_stdout)
        mkpipe( pipes[1], _stdout, 0,
                "trying to create the pipe to connect to child standard output" );

    if (_stderr && _stderr != _stdout)
        mkpipe( pipes[2], _stderr, 0,
                "trying to create the pipe to connect to child standard output" );

}

void ChildProcess::setupPrefork()
{
}

void ChildProcess::setupChild() {
    if (_stdin) {
        // Redirect input from the parent to _stdin
        if (close(pipes[0][1]) == -1)
            throw_system_error("closing write end of parent _stdin pipe");

        renamefd( pipes[0][0], STDIN_FILENO, "renaming parent _stdin pipe fd" );
    }

    if (_stdout) {
        // Redirect output to the parent _stdout fd
        if (close(pipes[1][0]) == -1)
            throw_system_error("closing read end of parent _stdout pipe");

        if (_stderr == _stdout)
            if (dup2(pipes[1][1], STDERR_FILENO) == -1)
                throw_system_error( "dup2-ing _stderr to parent _stdout/_stderr pipe");
        renamefd( pipes[1][1], STDOUT_FILENO, "renaming parent _stdout pipe" );
    }

    if (_stderr && _stderr != _stdout) {
        // Redirect all output to the parent
        if (close(pipes[2][0]) == -1)
            throw_system_error("closing read end of parent _stderr pipe");

        renamefd( pipes[2][1], STDERR_FILENO, "renaming parent _stderr pipe" );
    }
}

void ChildProcess::setupParent() {
    funlockfile(stdin);
    funlockfile(stderr);
    funlockfile(stdout);

    if (_stdin && close(pipes[0][0]) == -1)
        throw_system_error("closing read end of _stdin child pipe");
    if (_stdout && close(pipes[1][1]) == -1)
        throw_system_error("closing write end of _stdout child pipe");
    if (_stderr && _stderr != _stdout && close(pipes[2][1]) == -1)
        throw_system_error("closing write end of _stderr child pipe");
}

void ChildProcess::waitError() {
    if (errno == EINTR)
        throw std::runtime_error("system call interrupted while waiting for child termination");
    else
        throw_system_error("waiting for child termination");
}

bool ChildProcess::running()
{
    if ( _pid == -1 ) {
        return false;
    }

    int res = waitpid(_pid, &m_status, WNOHANG);

    if ( res == -1 ) {
        waitError();
    }

    if ( !res ) {
        return true;
    }

    return false;
}

int ChildProcess::wait(struct rusage* ru)
{
    if (_pid == -1)
        return -1; // FIXME: for lack of better ideas

    if (wait4(_pid, &m_status, 0, ru) == -1)
        waitError();

    _pid = -1;
    return m_status;
}

void ChildProcess::waitForSuccess() {
    int r = wait();
    if ( WIFEXITED( r ) ) {
        if ( WEXITSTATUS( r ) )
        {
            char buf[128];
            snprintf(buf, 128, "Subprocess terminated with error %d.", WEXITSTATUS(r));
            throw std::runtime_error(buf);
        }
        else
            return;
    }
    if ( WIFSIGNALED( r ) )
    {
        char buf[128];
        snprintf(buf, 128, "Subprocess terminated by signal %d.", WTERMSIG(r));
        throw std::runtime_error(buf);
    }
    throw exception::Generic( "Error waiting for subprocess." );
}

void ChildProcess::kill(int signal)
{
    if (_pid == -1)
        throw std::runtime_error("cannot kill child process: child process has not been started");
    if (::kill(_pid, signal) == -1)
    {
        stringstream str;
        str << "killing process " << _pid;
        throw_system_error(str.str());
    }
}

}
}
// vim:set ts=4 sw=4:
