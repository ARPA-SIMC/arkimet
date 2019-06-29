#ifndef ARKI_RUNTIME_TESTS_H
#define ARKI_RUNTIME_TESTS_H

#include <arki/core/file.h>
#include <initializer_list>
#include <vector>
#include <cstdio>

namespace arki {
namespace runtime {
namespace tests {

struct CatchOutput
{
    arki::core::File file_stdin;
    arki::core::File file_stdout;
    arki::core::File file_stderr;
    int orig_stdin;
    int orig_stdout;
    int orig_stderr;

    CatchOutput();
    CatchOutput(arki::core::File&& file_stdin);
    ~CatchOutput();

    int save(int src, int tgt);
    void restore(int src, int tgt);

    /**
     * Throw an exception if res is non-zero or stderr is not empty
     */
    void check_success(int res);
};


template<typename Cmd>
int run_cmdline(std::initializer_list<const char*> argv)
{
    std::vector<const char*> cmd_argv(argv);
    cmd_argv.push_back(nullptr);
    Cmd cmd;
    int res = cmd.run(cmd_argv.size() - 1, cmd_argv.data());
    // Flush stdio's buffers, so what was written gets written to the capture
    // files
    fflush(stdout);
    fflush(stderr);
    return res;
}

}
}
}

#endif
