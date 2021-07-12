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

}
}

#endif
