/*
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/tests/test-utils.h>
#include <arki/utils/process.h>
#include <wibble/string.h>
#include <cstdlib>
#include <sstream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_process_shar {
    string scriptdir;

    arki_utils_process_shar()
    {
        char* env_ppdir = getenv("ARKI_POSTPROC");
        if (env_ppdir)
            scriptdir = env_ppdir;
        else
            scriptdir = ".";
    }
};
TESTGRP(arki_utils_process);

// Send all input to the child; read child stdout to out
void send_and_capture_output(utils::FilterHandler& fh, const std::string& input, ostream& out)
{
    const void* buf = input.data();
    size_t size = input.size();

    while (true)
    {
        if (size > 0)
        {
            // Wait until there is data available, processing stderr in the meantime
            if (!fh.wait_for_child_data_while_sending(buf, size))
                fh.done_with_input();
            // TODO: test what happens when the child process closes stdin
            // without consuming all its input: sigpipe? exception?
        } else {
            fh.wait_for_child_data();
        }

        // Read data from child
        char buf[4096*2];
        ssize_t res = read(fh.outfd, buf, 4096*2);
        if (res < 0)
            throw wibble::exception::System("reading from child process");
        // TODO: test what happens when the child process closes stdout but
        // it's still reading input and passing it to stderr
        if (res == 0)
            return;

        // Pass it on
        out.write(buf, res);
    }
}

// Close output pipe to child, read all child output to out
void capture_output(utils::FilterHandler& fh, ostream& out)
{
    fh.done_with_input();

    while (true)
    {
        // Wait until there is data available, processing stderr in the meantime
        fh.wait_for_child_data();

        // Read data from child
        char buf[4096*2];
        ssize_t res = read(fh.outfd, buf, 4096*2);
        if (res < 0)
            throw wibble::exception::System("reading from child process");
        if (res == 0)
            return;
        // Pass it on
        out.write(buf, res);
    }
}

// Run process, send stdin, capture stdout and stderr, return its exit status
int run_and_capture(sys::ChildProcess& cp, const std::string& in, ostream& out, ostream& err)
{
    utils::FilterHandler fh(cp);
    fh.conf_errstream = &err;
    fh.start();

    if (in.empty())
        capture_output(fh, out);
    else
        send_and_capture_output(fh, in, out);

    return cp.wait();
}

// Process that just exits with success
template<> template<>
void to::test<1>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/true");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that just exits with error
template<> template<>
void to::test<2>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/false");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(WEXITSTATUS(res), 1);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(WEXITSTATUS(res), 1);
}

// Process that eats stdin, outputs nothing
template<> template<>
void to::test<3>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("cat >/dev/null");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that copies stdin to stdout
template<> template<>
void to::test<4>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/cat");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "This is input");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that copies stdin to stderr
template<> template<>
void to::test<5>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("cat >&2");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "This is input");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that copies stdin to stdout and stderr
template<> template<>
void to::test<6>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("tee /dev/stderr");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "This is input");
    // Nothing on stderr
    ensure_equals(err.str(), "This is input");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that outputs data to stdout without reading stdin
template<> template<>
void to::test<7>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("echo 'This is output'");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "This is output\n");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "This is output\n");
    // Nothing on stderr
    ensure_equals(err.str(), "");
    // Exit status should be success
    ensure_equals(res, 0);
}

// Process that outputs data to stdout without reading stdin
template<> template<>
void to::test<8>()
{
    utils::Subcommand cmd;
    cmd.args.push_back("/bin/sh");
    cmd.args.push_back("-c");
    cmd.args.push_back("echo 'This is output' >&2");

    stringstream out;
    stringstream err;
    int res = run_and_capture(cmd, string(), out, err);

    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "This is output\n");
    // Exit status should be success
    ensure_equals(res, 0);

    out.str(string());
    err.str(string());

    // Try again, with some input
    res = run_and_capture(cmd, "This is input", out, err);
    // Nothing on stdout
    ensure_equals(out.str(), "");
    // Nothing on stderr
    ensure_equals(err.str(), "This is output\n");
    // Exit status should be success
    ensure_equals(res, 0);
}

}

// vim:set ts=4 sw=4:
