#include "arki/tests/tests.h"
#include "process.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct IOCollector : public utils::IODispatcher
{
    stringstream out;
    stringstream err;

    using utils::IODispatcher::IODispatcher;

    void read_stdout() override
    {
        if (!fd_to_stream(subproc.get_stdout(), out))
            subproc.close_stdout();
    }

    void read_stderr() override
    {
        if (!fd_to_stream(subproc.get_stderr(), err))
            subproc.close_stderr();
    }

    void communicate(const std::string& data)
    {
        wassert(start());
        size_t res = send(data);
        subproc.close_stdin();
        wassert(actual(res) == data.size());
        wassert(flush());
    }

    void start()
    {
        subproc.set_stdin(subprocess::Redirect::PIPE);
        subproc.set_stdout(subprocess::Redirect::PIPE);
        subproc.set_stderr(subprocess::Redirect::PIPE);
        out.str(string());
        err.str(string());
        subproc.fork();
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
    {
        subprocess::Popen cmd({"/bin/true"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/true"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that just exits with error
add_method("false", [] {
    {
        subprocess::Popen cmd({"/bin/false"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 1);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/false"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 1);
    }
});

// Process that eats stdin, outputs nothing
add_method("devnull", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat >/dev/null"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat >/dev/null"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that copies stdin to stdout
add_method("cat", [] {
    {
        subprocess::Popen cmd({"/bin/cat"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/cat"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "This is input");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that copies stdin to stderr
add_method("cat_to_stderr", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat >&2"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat >&2"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "This is input");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that copies stdin to stdout and stderr
add_method("tee_to_stderr", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "tee /dev/stderr"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "tee /dev/stderr"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "This is input");
        wassert(actual(ioc.err.str()) == "This is input");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that outputs data to stdout without reading stdin
add_method("ignore_stdin_write_stdout", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "echo 'This is output'"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "This is output\n");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "echo 'This is output'"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "This is output\n");
        wassert(actual(ioc.err.str()) == "");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that outputs data to stderr without reading stdin
add_method("ignore_stdin_write_stderr", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "echo 'This is output' >&2"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "This is output\n");
        wassert(actual(cmd.wait()) == 0);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "echo 'This is output' >&2"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "This is output\n");
        wassert(actual(cmd.wait()) == 0);
    }
});

// Process that copies stdin to stdout then exits with an error
add_method("cat_then_fail", [] {
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat; echo 'FAIL' >&2; exit 1"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate(""));
        wassert(actual(ioc.out.str()) == "");
        wassert(actual(ioc.err.str()) == "FAIL\n");
        wassert(actual(cmd.wait()) == 1);
    }

    // Try again, with some input
    {
        subprocess::Popen cmd({"/bin/sh", "-c", "cat; echo 'FAIL' >&2; exit 1"});
        IOCollector ioc(cmd);
        wassert(ioc.communicate("This is input"));
        wassert(actual(ioc.out.str()) == "This is input");
        wassert(actual(ioc.err.str()) == "FAIL\n");
        wassert(actual(cmd.wait()) == 1);
    }
});

}

}
