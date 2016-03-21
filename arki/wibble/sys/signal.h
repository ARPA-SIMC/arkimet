// -*- C++ -*-
#ifndef WIBBLE_SYS_SIGNAL_H
#define WIBBLE_SYS_SIGNAL_H

#include <arki/exceptions.h>
#include <signal.h>

namespace wibble {
namespace sys {
namespace sig {

/**
 * RAII-style sigprocmask wrapper
 */
struct ProcMask
{
    sigset_t oldset;
    ProcMask(const sigset_t& newset, int how = SIG_BLOCK)
    {
        if (sigprocmask(how, &newset, &oldset) < 0)
            arki::throw_system_error("setting signal mask");
    }

    ~ProcMask()
    {
        if (sigprocmask(SIG_SETMASK, &oldset, NULL) < 0)
            arki::throw_system_error("restoring signal mask");
    }
};

struct Action
{
    int signum;
    struct sigaction oldact;

    Action(int signum, const struct sigaction& act) : signum(signum)
    {
        if (sigaction(signum, &act, &oldact) < 0)
            arki::throw_system_error("setting signal action");
    }
    ~Action()
    {
        if (sigaction(signum, &oldact, NULL) < 0)
            arki::throw_system_error("restoring signal action");
    }
};

}
}
}
#endif
