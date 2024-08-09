#include "config.h"
#include "runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/types-init.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/scan.h"
#include "arki/dataset/querymacro.h"
#include <algorithm>
#include <cmath>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

Config::Config()
{
    std::filesystem::path confdir(CONF_DIR);

    if (const char* envdir = getenv("ARKI_FORMATTER"))
        dir_formatter.push_back(envdir);
    dir_formatter.push_back(confdir / "format");

    if (const char* envdir = getenv("ARKI_BBOX"))
        dir_bbox.push_back(envdir);
    dir_bbox.push_back(confdir / "bbox");

    // TODO: colon-separated $PATH-like semantics
    if (const char* envdir = getenv("ARKI_POSTPROC"))
        dir_postproc.push_back(envdir);
    dir_postproc.push_back(POSTPROC_DIR);

    dir_qmacro.init_config_and_env("qmacro", "ARKI_QMACRO");
    if (const char* envdir = getenv("ARKI_SCAN"))
        dir_scan.push_back(envdir);
    if (const char* envdir = getenv("ARKI_SCAN_GRIB1"))
        dir_scan.push_back(envdir);
    if (const char* envdir = getenv("ARKI_SCAN_GRIB2"))
        dir_scan.push_back(envdir);
    dir_scan.push_back(confdir / "scan");
    dir_scan.push_back(confdir / "scan-grib1");
    dir_scan.push_back(confdir / "scan-grib2");
    dir_scan_bufr.init_config_and_env("scan-bufr", "ARKI_SCAN_BUFR");
    dir_scan_odimh5.init_config_and_env("scan-odimh5", "ARKI_SCAN_ODIMH5");

    if (const char* envfile = getenv("ARKI_IOTRACE"))
        file_iotrace_output = envfile;

    if (const char* envfile = getenv("ARKI_ALIASES"))
    {
        // Try environment first...
        file_aliases = envfile;
        if (!sys::stat(file_aliases))
            arki::nag::warning("%s: file specified in ARKI_ALIASES not found", file_aliases.c_str());
    }
#ifdef CONF_DIR
    else
    {
        // ...and build-time config otherwise
        file_aliases = confdir / "match-alias.conf";
    }
#endif

    if (const char* env = getenv("ARKI_IO_TIMEOUT"))
        io_timeout_ms = round(strtod(env, nullptr) * 1000.0);
}

Config& Config::get()
{
    static Config* instance = 0;
    if (!instance)
        instance = new Config;
    return *instance;
}

void Config::Dirlist::init_config_and_env(const char* confdir, const char* envname)
{
    // TODO: colon-separated $PATH-like semantics
    if (const char* envdir = getenv(envname))
        push_back(envdir);
    push_back(str::joinpath(CONF_DIR, confdir));
}

std::filesystem::path Config::Dirlist::find_file(const std::filesystem::path& fname, bool executable) const
{
    auto res = find_file_noerror(fname, executable);
    if (res.empty())
    {
        stringstream s;
        s << (executable ? "program" : "file") << " " << fname << " not found; tried: " << str::join(" ", begin(), end());
        // Build a nice error message
        throw std::runtime_error(s.str());
    }
    else
        return res;
}

std::filesystem::path Config::Dirlist::find_file_noerror(const std::filesystem::path& fname, bool executable) const
{
    int mode = executable ? X_OK : F_OK;
    for (const_iterator i = begin(); i != end(); ++i)
    {
        string res = str::joinpath(*i, fname);
        if (sys::access(res, mode))
            return res;
    }
    return std::string();
}

std::vector<std::filesystem::path> Config::Dirlist::list_files(const std::string& ext, bool first_only) const
{
    std::vector<filesystem::path> res;

    for (const auto& path: *this)
    {
        if (!std::filesystem::is_directory(path))
            continue;
        std::vector<std::filesystem::path> files;
        sys::Path dir(path);
        for (auto di = dir.begin(); di != dir.end(); ++di)
        {
            std::string file = di->d_name;
            // Skip hidden files
            if (file[0] == '.') continue;
            // Skip files with different ending
            if (not str::endswith(file, ext)) continue;
            // Skip non-files
            if (!di.isreg()) continue;
            files.push_back(file);
        }

        // Sort the file names
        std::sort(files.begin(), files.end());

        // Append the sorted file list to the result
        for (const auto& fn: files)
            res.push_back(path / fn);

        // Stop here if we got results and only a dir is needed
        if (!res.empty() && first_only)
            break;
    }

    return res;
}


void init()
{
    static bool initialized = false;

    if (initialized) return;
    types::init_default_types();
    iotrace::init();
    scan::init();
    dataset::qmacro::init();
    initialized = true;
}

}
