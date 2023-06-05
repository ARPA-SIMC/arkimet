#include "filter.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki {
namespace stream {

FilterProcess::FilterProcess(const std::vector<std::string>& args, int timeout_ms)
    : timeout_ms(timeout_ms)
{
    cmd.args = args;
    cmd.set_stdin(utils::subprocess::Redirect::PIPE);
    cmd.set_stdout(utils::subprocess::Redirect::PIPE);
    cmd.set_stderr(utils::subprocess::Redirect::PIPE);
}


void FilterProcess::start()
{
    // Spawn the command
    cmd.fork();

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

void FilterProcess::stop()
{
    if (timeout_ms == -1)
    {
        cmd.wait();
        return;
    } else if (!cmd.wait(timeout_ms))
        terminate();
}

void FilterProcess::terminate()
{
    if (!cmd.started())
        return;

    cmd.terminate();

    if (timeout_ms == -1)
    {
        cmd.wait();
        return;
    } else if (!cmd.wait(timeout_ms)) {
        cmd.kill();

        if (!cmd.wait(timeout_ms))
            throw std::runtime_error("failed to terminate the child process");
    }
}

void FilterProcess::check_for_errors()
{
    int res = cmd.raw_returncode();
    if (res)
    {
        std::string msg = "cannot run postprocessing filter: postprocess command \"" + str::join(" ", cmd.args) + "\" " + subprocess::Child::format_raw_returncode(res);
        if (!errors.str().empty())
            msg += "; stderr: " + str::strip(errors.str());
        throw std::runtime_error(msg);
    }
}

}
}
