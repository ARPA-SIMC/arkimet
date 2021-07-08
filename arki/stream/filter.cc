#include "filter.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki {
namespace stream {

FilterProcess::FilterProcess(const std::vector<std::string>& args) : utils::IODispatcher(cmd)
{
    cmd.args = args;
    cmd.set_stdin(utils::subprocess::Redirect::PIPE);
    cmd.set_stdout(utils::subprocess::Redirect::PIPE);
    cmd.set_stderr(utils::subprocess::Redirect::PIPE);

    m_err = &errors;
}


void FilterProcess::start() {
    // Spawn the command
    subproc.fork();

    // set the stdin/stdout/stderr fds to nonblocking
    sys::FileDescriptor child_stdin(cmd.get_stdin());
    if (child_stdin != -1)
        child_stdin.setfl(child_stdin.getfl() | O_NONBLOCK);
    sys::FileDescriptor child_stdout(cmd.get_stdout());
    if (child_stdout != -1)
        child_stdout.setfl(child_stdout.getfl() | O_NONBLOCK);
    sys::FileDescriptor child_stderr(cmd.get_stderr());
    if (child_stderr != -1)
        child_stderr.setfl(child_stderr.getfl() | O_NONBLOCK);
}

}
}
