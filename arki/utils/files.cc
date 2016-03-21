#include "config.h"
#include "arki/utils.h"
#include "files.h"
#include "sys.h"
#include "string.h"
#include <deque>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>

using namespace std;

namespace arki {
namespace utils {
namespace files {

void createRebuildFlagfile(const std::string& pathname)
{
	utils::createFlagfile(pathname + FLAGFILE_REBUILD);
}
void createNewRebuildFlagfile(const std::string& pathname)
{
	utils::createNewFlagfile(pathname + FLAGFILE_REBUILD);
}
void removeRebuildFlagfile(const std::string& pathname)
{
    sys::unlink_ifexists(pathname + FLAGFILE_REBUILD);
}
bool hasRebuildFlagfile(const std::string& pathname)
{
    return sys::exists(pathname + FLAGFILE_REBUILD);
}


void createPackFlagfile(const std::string& pathname)
{
	utils::createFlagfile(pathname + FLAGFILE_PACK);
}
void createNewPackFlagfile(const std::string& pathname)
{
	utils::createNewFlagfile(pathname + FLAGFILE_PACK);
}
void removePackFlagfile(const std::string& pathname)
{
    sys::unlink_ifexists(pathname + FLAGFILE_PACK);
}
bool hasPackFlagfile(const std::string& pathname)
{
    return sys::exists(pathname + FLAGFILE_PACK);
}


void createIndexFlagfile(const std::string& dir)
{
	utils::createFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
}
void createNewIndexFlagfile(const std::string& dir)
{
	utils::createNewFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
}
void removeIndexFlagfile(const std::string& dir)
{
    sys::unlink_ifexists(str::joinpath(dir, FLAGFILE_INDEX));
}
bool hasIndexFlagfile(const std::string& dir)
{
    return sys::exists(str::joinpath(dir, FLAGFILE_INDEX));
}


void createDontpackFlagfile(const std::string& dir)
{
	utils::createFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void createNewDontpackFlagfile(const std::string& dir)
{
	utils::createNewFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void removeDontpackFlagfile(const std::string& dir)
{
    sys::unlink_ifexists(str::joinpath(dir, FLAGFILE_DONTPACK));
}
bool hasDontpackFlagfile(const std::string& dir)
{
    return sys::exists(str::joinpath(dir, FLAGFILE_DONTPACK));
}

std::string read_file(const std::string &file)
{
    if (file == "-")
    {
        static const size_t bufsize = 4096;
        char buf[bufsize];
        std::string res;
        sys::NamedFileDescriptor in(0, "(stdin)");
        while (true)
        {
            size_t count = in.read(buf, bufsize);
            if (count == 0) break;
            res.append(buf, count);
        }
        return res;
    }
    else
        return sys::read_file(file);
}

void resolve_path(const std::string& pathname, std::string& basedir, std::string& relname)
{
    if (pathname[0] == '/')
        basedir.clear();
    else
        basedir = sys::getcwd();
    relname = str::normpath(pathname);
}

string normaliseFormat(const std::string& format)
{
    string f = str::lower(format);
    if (f == "metadata") return "arkimet";
    if (f == "grib1") return "grib";
    if (f == "grib2") return "grib";
#ifdef HAVE_HDF5
    if (f == "h5")     return "odimh5";
    if (f == "hdf5")   return "odimh5";
    if (f == "odim")   return "odimh5";
    if (f == "odimh5") return "odimh5";
#endif
    return f;
}

std::string format_from_ext(const std::string& fname, const char* default_format)
{
    // Extract the extension
    size_t epos = fname.rfind('.');
    if (epos != string::npos)
        return normaliseFormat(fname.substr(epos + 1));
    else if (default_format)
        return default_format;
    else
    {
        stringstream ss;
        ss << "cannot auto-detect format from file name " << fname << ": file extension not recognised";
        throw std::runtime_error(ss.str());
    }
}

PreserveFileTimes::PreserveFileTimes(const std::string& fname)
    : fname(fname)
{
    struct stat st;
    sys::stat(fname, st);
    times[0] = st.st_atim;
    times[1] = st.st_mtim;
}

PreserveFileTimes::~PreserveFileTimes()
{
    if (utimensat(AT_FDCWD, fname.c_str(), times, 0) == -1)
        throw std::system_error(errno, std::system_category(), "cannot set file times");
}

}
}
}
