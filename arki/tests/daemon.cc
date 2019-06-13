#include "daemon.h"
#include "arki/utils/sys.h"
#include <cstring>
#include <sstream>

using namespace arki::utils;

namespace arki {
namespace tests {

Daemon::Daemon(std::initializer_list<std::string> args)
{
    this->args = args;
    const char* top_srcdir = getenv("TOP_SRCDIR");
    if (top_srcdir)
        this->args[0] = std::string(top_srcdir) + "/" + this->args[0];
    fprintf(::stderr, "RUN %s\n", this->args[0].c_str());

    this->set_stdout(subprocess::Redirect::PIPE);
    fork();
    sys::NamedFileDescriptor in(this->get_stdout(), "child stdout");
    char buf[128];
    size_t size = in.read(buf, 128);
    buf[size] = 0;
    if (strncmp(buf, "OK", 2) != 0)
    {
        std::stringstream msg;
        msg << "Child process reported '" << buf << "' instead of 'OK'";
        throw std::runtime_error(msg.str());
    }
    daemon_start_string = buf;
}

Daemon::~Daemon()
{
    if (!terminated())
    {
        terminate();
        wait();
    }
}

}
}
