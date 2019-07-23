#include "config.h"
#include "runtime.h"
#include "arki/matcher/aliases.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

Config::Config()
{
    // TODO: colon-separated $PATH-like semantics
    if (const char* envdir = getenv("ARKI_POSTPROC"))
        dir_postproc.push_back(envdir);
    dir_postproc.push_back(POSTPROC_DIR);

    dir_report.init_config_and_env("report", "ARKI_REPORT");
    dir_qmacro.init_config_and_env("qmacro", "ARKI_QMACRO");
    dir_scan_grib1.init_config_and_env("scan-grib1", "ARKI_SCAN_GRIB1");
    dir_scan_grib2.init_config_and_env("scan-grib2", "ARKI_SCAN_GRIB2");
    dir_scan_bufr.init_config_and_env("scan-bufr", "ARKI_SCAN_BUFR");
    dir_scan_odimh5.init_config_and_env("scan-odimh5", "ARKI_SCAN_ODIMH5");
    dir_targetfile.init_config_and_env("targetfile", "ARKI_TARGETFILE");

    if (const char* envfile = getenv("ARKI_IOTRACE"))
        file_iotrace_output = envfile;

    if (const char* envfile = getenv("ARKI_VM2_FILE"))
        file_vm2_config = envfile;
}

static void describe_envvar(ostream& out, const char* envvar)
{
    out << "  change with " << envvar << " (currently ";
    if (const char* envdir = getenv(envvar))
        out << "'" << envdir << "'";
    else
        out << "unset";
    out << ")" << endl;
}

void Config::describe(std::ostream& out) const
{
    dir_postproc.describe(out, "Postprocessors", "ARKI_POSTPROC");
    dir_report.describe(out, "Report scripts", "ARKI_REPORT");
    dir_qmacro.describe(out, "Query macro scripts", "ARKI_QMACRO");
    dir_scan_grib1.describe(out, "GRIB1 scan scripts", "ARKI_SCAN_GRIB1");
    dir_scan_grib2.describe(out, "GRIB2 scan scripts", "ARKI_SCAN_GRIB2");
    dir_scan_bufr.describe(out, "BUFR scan scripts", "ARKI_SCAN_BUFR");
    dir_targetfile.describe(out, "Target file name scripts", "ARKI_TARGETFILE");

    out << "I/O profiling: ";
#ifdef ARKI_IOTRACE
    if (file_iotrace_output.empty())
        out << "disabled." << endl;
    else
        out << "logged to " << file_iotrace_output << endl;
    describe_envvar(out, "ARKI_IOTRACE");
#else
    out << "disabled at compile time." << endl;
#endif
}

Config& Config::get()
{
    static Config* instance = 0;
    if (!instance)
        instance = new Config;
    return *instance;
}

void Config::Dirlist::describe(ostream& out, const char* desc, const char* envvar) const
{
    out << desc << ": ";
    out << str::join(":", begin(), end());
    out << endl;
    if (envvar) describe_envvar(out, envvar);
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

    for (const_iterator i = begin(); i != end(); ++i)
    {
        vector<string> files;
        sys::Path dir(*i);
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
        for (vector<string>::const_iterator fn = files.begin();
                fn != files.end(); ++fn)
            res.push_back(str::joinpath(*i, *fn));

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
    matcher::read_matcher_alias_database();
    iotrace::init();
    initialized = true;
}

}
