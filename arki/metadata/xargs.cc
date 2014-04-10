/*
 * metadata/xargs - Cluster a metadata stream and run a progrgam on each batch
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include "xargs.h"
#include <arki/metadata.h>
#include <arki/utils/raii.h>
#include <arki/dataset/data.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace wibble;

extern char **environ;

namespace arki {
namespace metadata {

namespace xargs {

Tempfile::Tempfile()
    : pathname(0), fd(-1)
{
}

Tempfile::~Tempfile()
{
    close_nothrow();
}

void Tempfile::set_template(const std::string& tpl)
{
    if (fd != -1)
        throw wibble::exception::Consistency(
                "setting a new temp file template",
                "there is already a temp file open");

    pathname_template = tpl;
}

void Tempfile::open()
{
    if (fd != -1)
        throw wibble::exception::Consistency(
                "opening temp file for new batch",
                "there is already a tempfile open");

    utils::raii::TransactionAllocArray<char> allocpn(
            pathname,
            pathname_template.size() + 1,
            pathname_template.c_str());
    fd = mkstemp(pathname);
    if (fd < 0)
    {
        throw wibble::exception::System("creating temporary file");
    }
    allocpn.commit();
}

void Tempfile::close_nothrow()
{
    if (fd != -1)
    {
        ::close(fd);
        fd = -1;
    }
    if (pathname)
    {
        delete[] pathname;
        pathname = 0;
    }
}

void Tempfile::close()
{
    if (fd == -1) return;

    // Use RAII so we always reset
    utils::raii::SetOnExit<int> xx1(fd, -1);
    utils::raii::DeleteArrayAndZeroOnExit<char> xx2(pathname);

    if (::close(fd) < 0)
    {
        throw wibble::exception::File(pathname, "closing file");
    }

    // delete the file, if it still exists
    sys::fs::deleteIfExists(pathname);
}

bool Tempfile::is_open() const
{
    return fd != -1;
}

}

Xargs::Xargs()
    : metadata::Clusterer(), filename_argument(-1)
{
    const char* tmpdir = getenv("TMPDIR");

    if (tmpdir)
    {
        tempfile.set_template(str::joinpath(tmpdir, "arki-xargs.XXXXXX"));
    } else {
        tempfile.set_template("/tmp/arki-xargs.XXXXXX");
    }
}

void Xargs::start_batch(const std::string& new_format)
{
    metadata::Clusterer::start_batch(new_format);
    tempfile.open();
}

void Xargs::add_to_batch(Metadata& md, sys::Buffer& buf)
{
    metadata::Clusterer::add_to_batch(md, buf);
    arki::dataset::data::OstreamWriter::get(md.source->format)->stream(md, tempfile.fd);
}

void Xargs::flush_batch()
{
    if (!tempfile.is_open()) return;
    int res;

    try {
        // Run process with fname as parameter
        res = run_child();
    } catch (...) {
        // Ignore close exceptions here, so we rethrow what really happened
        tempfile.close_nothrow();
        throw;
    }

    tempfile.close();
    metadata::Clusterer::flush_batch();

    if (res != 0)
        throw wibble::exception::Consistency("running " + command[0], "process returned exit status " + str::fmt(res));
}

int Xargs::run_child()
{
    struct CustomChild : public sys::Exec
    {
        CustomChild(const std::string& pathname) : sys::Exec(pathname) {}

        virtual int main()
        {
            // Redirect stdin to /dev/null
            int new_stdin = open("/dev/null", O_RDONLY);
            ::dup2(new_stdin, 0);
            sys::Exec::main();
        }
    };

    if (count == 0) return 0;

    CustomChild child(command[0]);
    child.args = command;
    if (filename_argument == -1)
        child.args.push_back(tempfile.pathname);
    else {
        if ((unsigned)filename_argument < child.args.size())
            child.args[filename_argument] = tempfile.pathname;
    }
    child.searchInPath = true;
    child.envFromParent = false;

    // Import all the environment except ARKI_XARGS_* variables
    for (char** s = environ; *s; ++s)
    {
        string envstr(*s);
        if (str::startsWith(envstr, "ARKI_XARGS_")) continue;
        child.env.push_back(envstr);
    }
    child.env.push_back("ARKI_XARGS_FILENAME=" + string(tempfile.pathname));
    child.env.push_back("ARKI_XARGS_FORMAT=" + str::toupper(format));
    child.env.push_back("ARKI_XARGS_COUNT=" + str::fmt(count));

    if (timespan.begin.defined())
    {
        child.env.push_back("ARKI_XARGS_TIME_START=" + timespan.begin->toISO8601(' '));
        if (timespan.end.defined())
            child.env.push_back("ARKI_XARGS_TIME_END=" + timespan.end->toISO8601(' '));
        else
            child.env.push_back("ARKI_XARGS_TIME_END=" + timespan.begin->toISO8601(' '));
    }

    child.fork();
    return child.wait();
}

static size_t parse_size(const std::string& str)
{
    const char* s = str.c_str();
    char* e;
    unsigned long long int res = strtoull(s, &e, 10);
    string suffix = str.substr(e-s);
    if (suffix.empty() || suffix == "c")
        return res;
    if (suffix == "w") return res * 2;
    if (suffix == "b") return res * 512;
    if (suffix == "kB") return res * 1000;
    if (suffix == "K") return res * 1024;
    if (suffix == "MB") return res * 1000*1000;
    if (suffix == "M" || suffix == "xM") return res * 1024*1024;
    if (suffix == "GB") return res * 1000*1000*1000;
    if (suffix == "G") return res * 1024*1024*1024;
    if (suffix == "TB") return res * 1000*1000*1000*1000;
    if (suffix == "T") return res * 1024*1024*1024*1024;
    if (suffix == "PB") return res * 1000*1000*1000*1000*1000;
    if (suffix == "P") return res * 1024*1024*1024*1024*1024;
    if (suffix == "EB") return res * 1000*1000*1000*1000*1000*1000;
    if (suffix == "E") return res * 1024*1024*1024*1024*1024*1024;
    if (suffix == "ZB") return res * 1000*1000*1000*1000*1000*1000*1000;
    if (suffix == "Z") return res * 1024*1024*1024*1024*1024*1024*1024;
    if (suffix == "YB") return res * 1000*1000*1000*1000*1000*1000*1000*1000;
    if (suffix == "Y") return res * 1024*1024*1024*1024*1024*1024*1024*1024;
    throw wibble::exception::Consistency("parsing size", "unknown suffix: '"+suffix+"'");
}

static size_t parse_interval(const std::string& str)
{
    string name = str::trim(str::tolower(str));
    if (name == "minute") return 5;
    if (name == "hour") return 4;
    if (name == "day") return 3;
    if (name == "month") return 2;
    if (name == "year") return 1;
    throw wibble::exception::Consistency("parsing interval name", "unsupported interval: " + str + ".  Valid intervals are minute, hour, day, month and year");
}

void Xargs::set_max_bytes(const std::string& val)
{
    max_bytes = parse_size(val);
}

void Xargs::set_interval(const std::string& val)
{
    max_interval = parse_interval(val);
}

}
}

// vim:set ts=4 sw=4:
