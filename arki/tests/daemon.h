#ifndef ARKI_TEST_DAEMON_H
#define ARKI_TEST_DAEMON_H

#include <vector>
#include <string>
#include <sys/types.h>
#include <arki/utils/subprocess.h>

namespace arki {
namespace tests {

/**
 * Keep a subprocess active while this object is in scope.
 */
struct Daemon : public utils::subprocess::Popen
{
    std::string daemon_start_string;

    Daemon(std::initializer_list<std::string> args);
    ~Daemon();
};

}
}

#endif
