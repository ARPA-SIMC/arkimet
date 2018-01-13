#include "xargs.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/wibble/sys/exec.h"
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;

extern char **environ;

namespace arki {
namespace metadata {

Xargs::Xargs()
    : metadata::Clusterer(), tempfile(""), filename_argument(-1)
{
    const char* tmpdir = getenv("TMPDIR");

    if (tmpdir)
    {
        tempfile_template = str::joinpath(tmpdir, "arki-xargs.XXXXXX");
    } else {
        tempfile_template = "/tmp/arki-xargs.XXXXXX";
    }
}

void Xargs::start_batch(const std::string& new_format)
{
    metadata::Clusterer::start_batch(new_format);

    // Make a mutable copy of tempfile_template
    unique_ptr<char[]> tf(new char[tempfile_template.size() + 1]);
    memcpy(tf.get(), tempfile_template.c_str(), tempfile_template.size() + 1);
    tempfile = File::mkstemp(tf.get());
}

void Xargs::add_to_batch(Metadata& md, const std::vector<uint8_t>& buf)
{
    metadata::Clusterer::add_to_batch(md, buf);
    md.stream_data(tempfile);
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
        ::close(tempfile);
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
        child.args.push_back(tempfile.name());
    else {
        if ((unsigned)filename_argument < child.args.size())
            child.args[filename_argument] = tempfile.name();
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
    child.env.push_back("ARKI_XARGS_FILENAME=" + tempfile.name());
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
