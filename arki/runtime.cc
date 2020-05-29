#include "config.h"
#include "runtime.h"
#include "arki/matcher/aliases.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"
#include "arki/scan.h"
#include "arki/dataset/querymacro.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

Config::Config()
{
    if (const char* envdir = getenv("ARKI_FORMATTER"))
        dir_formatter.push_back(envdir);
    dir_formatter.push_back(std::string(CONF_DIR) + "/format");

    if (const char* envdir = getenv("ARKI_BBOX"))
        dir_bbox.push_back(envdir);
    dir_bbox.push_back(std::string(CONF_DIR) + "/bbox");

    // TODO: colon-separated $PATH-like semantics
    if (const char* envdir = getenv("ARKI_POSTPROC"))
        dir_postproc.push_back(envdir);
    dir_postproc.push_back(POSTPROC_DIR);

    dir_report.init_config_and_env("report", "ARKI_REPORT");
    dir_qmacro.init_config_and_env("qmacro", "ARKI_QMACRO");
    if (const char* envdir = getenv("ARKI_SCAN"))
        dir_scan.push_back(envdir);
    if (const char* envdir = getenv("ARKI_SCAN_GRIB1"))
        dir_scan.push_back(envdir);
    if (const char* envdir = getenv("ARKI_SCAN_GRIB2"))
        dir_scan.push_back(envdir);
    dir_scan.push_back(str::joinpath(CONF_DIR, "scan"));
    dir_scan.push_back(str::joinpath(CONF_DIR, "scan-grib1"));
    dir_scan.push_back(str::joinpath(CONF_DIR, "scan-grib2"));
    dir_scan_bufr.init_config_and_env("scan-bufr", "ARKI_SCAN_BUFR");
    dir_scan_odimh5.init_config_and_env("scan-odimh5", "ARKI_SCAN_ODIMH5");

    if (const char* envfile = getenv("ARKI_IOTRACE"))
        file_iotrace_output = envfile;

    if (const char* envfile = getenv("ARKI_VM2_FILE"))
        file_vm2_config = envfile;
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

std::string Config::Dirlist::find_file(const std::string& fname, bool executable) const
{
    string res = find_file_noerror(fname, executable);
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

std::string Config::Dirlist::find_file_noerror(const std::string& fname, bool executable) const
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

std::vector<std::string> Config::Dirlist::list_files(const std::string& ext, bool first_only) const
{
    vector<string> res;

    for (const auto& path: *this)
    {
        if (!sys::isdir(path))
            continue;
        std::vector<std::string> files;
        sys::Path dir(path);
        for (auto di = dir.begin(); di != dir.end(); ++di)
        {
            string file = di->d_name;
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
            res.push_back(str::joinpath(path, fn));

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
