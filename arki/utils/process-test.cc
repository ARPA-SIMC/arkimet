#include "arki/tests/tests.h"
#include "process.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct IOCollector : public utils::IODispatcher
{
    stringstream out;
    stringstream err;

    IOCollector(wibble::sys::ChildProcess& subproc)
        : utils::IODispatcher(subproc)
    {
    }

    virtual void read_stdout()
    {
        if (!fd_to_stream(outfd, out))
            close_outfd();
    }

    virtual void read_stderr()
    {
        if (!fd_to_stream(errfd, err))
            close_errfd();
    }

    size_t send_and_close(const std::string& data)
    {
        size_t res = send(data);
        close_infd();
        return res;
    }

    void start()
    {
        out.str(string());
        err.str(string());
        utils::IODispatcher::start();
    }
};


class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_process");

void Tests::register_tests() {

// Process that just exits with success
add_method("true", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/true");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);
});

// Process that just exits with error
add_method("false", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/false");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    int res = cmd.wait();
    ensure_equals(WEXITSTATUS(res), 1);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    res = cmd.wait();
    ensure_equals(WEXITSTATUS(res), 1);
});

// Process that eats stdin, outputs nothing
add_method("devnull", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("cat >/dev/null");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);
});

// Process that copies stdin to stdout
add_method("cat", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/cat");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "This is input");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);
});

// Process that copies stdin to stderr
add_method("cat_to_stderr", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("cat >&2");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "This is input");
    ensure_equals(cmd.wait(), 0);
});

// Process that copies stdin to stdout and stderr
add_method("tee_to_stderr", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("tee /dev/stderr");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "This is input");
    ensure_equals(ioc.err.str(), "This is input");
    ensure_equals(cmd.wait(), 0);
});

// Process that outputs data to stdout without reading stdin
add_method("ignore_stdin_write_stdout", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("echo 'This is output'");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "This is output\n");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "This is output\n");
    ensure_equals(ioc.err.str(), "");
    ensure_equals(cmd.wait(), 0);
});

// Process that outputs data to stderr without reading stdin
add_method("ignore_stdin_write_stderr", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("echo 'This is output' >&2");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "This is output\n");
    ensure_equals(cmd.wait(), 0);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "This is output\n");
    ensure_equals(cmd.wait(), 0);
});

// Process that copies stdin to stdout then exits with an error
add_method("cat_then_fail", [] {
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("cat; echo 'FAIL' >&2; exit 1");

    IOCollector ioc(cmd);
    ioc.start();
    ensure_equals(ioc.send_and_close(""), 0u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "");
    ensure_equals(ioc.err.str(), "FAIL\n");
    int res = cmd.wait();
    ensure_equals(WEXITSTATUS(res), 1);

    // Try again, with some input
    ioc.start();
    ensure_equals(ioc.send_and_close("This is input"), 13u);
    ioc.flush();
    ensure_equals(ioc.out.str(), "This is input");
    ensure_equals(ioc.err.str(), "FAIL\n");
    res = cmd.wait();
    ensure_equals(WEXITSTATUS(res), 1);
});

}

}
