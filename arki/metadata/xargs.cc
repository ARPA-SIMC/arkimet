#include "xargs.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/utils/raii.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/dataset/segment.h"
#include "arki/wibble/sys/exec.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;

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
        throw std::runtime_error("cannot set a new temp file template: there is already a temp file open");

    pathname_template = tpl;
}

void Tempfile::open()
{
    if (fd != -1)
        throw std::runtime_error("cannot open temp file for new batch: there is already a tempfile open");

    utils::raii::TransactionAllocArray<char> allocpn(
            pathname,
            pathname_template.size() + 1,
            pathname_template.c_str());
    fd = mkstemp(pathname);
    if (fd < 0)
        throw_system_error("cannot create temporary file");
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
        throw_file_error(pathname, "cannot close file");

    // delete the file, if it still exists
    sys::unlink_ifexists(pathname);
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

void Xargs::add_to_batch(Metadata& md, const std::vector<uint8_t>& buf)
{
    metadata::Clusterer::add_to_batch(md, buf);
    NamedFileDescriptor out(tempfile.fd, tempfile.pathname);
    arki::dataset::segment::OstreamWriter::get(md.source().format)->stream(md, out);
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
    {
        stringstream ss;
        ss << "cannot run " << command[0] << ": process returned exit status " << res;
        throw std::runtime_error(ss.str());
    }
}

int Xargs::run_child()
{
    struct CustomChild : public wibble::sys::Exec
    {
        CustomChild(const std::string& pathname) : wibble::sys::Exec(pathname) {}

        virtual int main()
        {
            // Redirect stdin to /dev/null
            int new_stdin = open("/dev/null", O_RDONLY);
            ::dup2(new_stdin, 0);
            return wibble::sys::Exec::main();
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
        if (str::startswith(envstr, "ARKI_XARGS_")) continue;
        child.env.push_back(envstr);
    }
    child.env.push_back("ARKI_XARGS_FILENAME=" + string(tempfile.pathname));
    child.env.push_back("ARKI_XARGS_FORMAT=" + str::upper(format));
    char buf[32];
    snprintf(buf, 32, "ARKI_XARGS_COUNT=%zd", count);
    child.env.push_back(buf);

    if (timespan_begin.get())
    {
        child.env.push_back("ARKI_XARGS_TIME_START=" + timespan_begin->to_iso8601(' '));
        if (timespan_end.get())
            child.env.push_back("ARKI_XARGS_TIME_END=" + timespan_end->to_iso8601(' '));
        else
            child.env.push_back("ARKI_XARGS_TIME_END=" + timespan_begin->to_iso8601(' '));
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
    throw std::runtime_error("cannot parse size: unknown suffix: '"+suffix+"'");
}

static size_t parse_interval(const std::string& str)
{
    string name = str::lower(str::strip(str));
    if (name == "minute") return 5;
    if (name == "hour") return 4;
    if (name == "day") return 3;
    if (name == "month") return 2;
    if (name == "year") return 1;
    throw std::runtime_error("cannot parse interval name: unsupported interval: " + str + ".  Valid intervals are minute, hour, day, month and year");
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
