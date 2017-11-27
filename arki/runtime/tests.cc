#include "tests.h"
#include "arki/exceptions.h"
#include <string>
#include <vector>

using namespace std;

namespace arki {
namespace runtime {
namespace tests {

CatchOutput::CatchOutput()
    : file_stdin(File::mkstemp(".")),
      file_stdout(File::mkstemp(".")),
      file_stderr(File::mkstemp("."))
{
    orig_stdin = save(file_stdin, 0);
    orig_stdout = save(file_stdout, 1);
    orig_stderr = save(file_stderr, 2);
}

CatchOutput::~CatchOutput()
{
    restore(orig_stdin, 0);
    restore(orig_stdout, 1);
    restore(orig_stderr, 2);
}

int CatchOutput::save(int src, int tgt)
{
    int res = ::dup(tgt);
    if (res == -1)
        throw_system_error("cannot dup file descriptor " + to_string(tgt));
    if (::dup2(src, tgt) == -1)
        throw_system_error("cannot dup2 file descriptor " + to_string(src) + " to " + to_string(tgt));
    return res;
}

void CatchOutput::restore(int src, int tgt)
{
    if (::dup2(src, tgt) == -1)
        throw_system_error("cannot dup2 file descriptor " + to_string(src) + " to " + to_string(tgt));
    if (::close(src) == -1)
        throw_system_error("cannot close saved file descriptor " + to_string(src));
}

int run_cmdline(cmdline_func func, std::initializer_list<const char*> argv)
{
    std::vector<const char*> cmd_argv(argv);
    cmd_argv.push_back(nullptr);
    return func(cmd_argv.size() - 1, cmd_argv.data());
}

}
}
}
