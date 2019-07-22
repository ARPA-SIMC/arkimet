#include "arki/runtime/config.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/utils.h"
#include "arki/matcher.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <algorithm>
#include <memory>
#include <unistd.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace runtime {

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

core::cfg::Sections parse_config_file(const std::string& file_name)
{
    if (file_name == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Sections::parse(in);
    }

    // Remove trailing slashes, if any
    string fname = file_name;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    // Check if it's a file or a directory
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == 0)
        throw std::runtime_error("cannot read configuration from " + fname + ": it does not exist");
    if (S_ISDIR(st->st_mode))
    {
        // If it's a directory, merge in its config file
        string name = str::basename(fname);
        string file = str::joinpath(fname, "config");

        // Parse the file
        sys::File in(file, O_RDONLY);
        auto section = core::cfg::Section::parse(in);
        // Fill in missing bits
        section.set("name", name);
        section.set("path", sys::abspath(fname));

        core::cfg::Sections sections;
        sections.emplace(std::move(name), std::move(section));
        return sections;
    } else {
        // If it's a file, then it's a merged config file
        sys::File in(fname, O_RDONLY);
        return core::cfg::Sections::parse(in);
    }
}

std::set<std::string> parseRestrict(const std::string& str)
{
	set<string> res;
	string cur;
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		if (isspace(*i) || *i == ',')
		{
			if (!cur.empty())
			{
				res.insert(cur);
				cur.clear();
			}
			continue;
		} else
			cur += *i;
	}
	if (!cur.empty())
		res.insert(cur);
	return res;
}

bool Restrict::is_allowed(const std::string& str) const
{
    if (wanted.empty()) return true;
    return is_allowed(parseRestrict(str));

}

bool Restrict::is_allowed(const std::set<std::string>& names) const
{
    if (wanted.empty()) return true;
    for (const auto& i: wanted)
        if (names.find(i) != names.end())
            return true;
    return false;
}

bool Restrict::is_allowed(const core::cfg::Section& cfg) const
{
    if (wanted.empty()) return true;
    return is_allowed(parseRestrict(cfg.value("restrict")));
}

void readMatcherAliasDatabase()
{
    // Otherwise the file given in the environment variable ARKI_ALIASES is tried.
    char* fromEnv = getenv("ARKI_ALIASES");
    if (fromEnv)
    {
        MatcherAliasDatabase::addGlobal(parse_config_file(fromEnv));
        return;
    }

#ifdef CONF_DIR
    // Else, CONF_DIR is tried.
    string name = string(CONF_DIR) + "/match-alias.conf";
    unique_ptr<struct stat> st = sys::stat(name);
    if (st.get())
    {
        MatcherAliasDatabase::addGlobal(parse_config_file(name));
        return;
    }
#endif

    // Else, nothing is loaded.
}

static std::string rcDirName(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    std::string dirname;
    char* fromEnv = 0;

    // Otherwise the directory given in the environment variable nameInEnv is tried.
    if ((fromEnv = getenv(nameInEnv.c_str())))
        return fromEnv;

#ifdef CONF_DIR
	// Else, CONF_DIR is tried.
	return string(CONF_DIR) + "/" + nameInConfdir;
#else
	// Or if there is no config, we fail to read anything
	return string();
#endif
}

std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    std::string dirname = rcDirName(nameInConfdir, nameInEnv);

    vector<string> files;
    sys::Path dir(dirname);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        string file = i->d_name;
        // Skip hidden files
        if (file[0] == '.') continue;
        // Skip backup files
        if (file[file.size() - 1] == '~') continue;
        // Skip non-files
        if (!i.isreg()) continue;
        files.push_back(str::joinpath(dirname, i->d_name));
    }

	// Sort the file names
	std::sort(files.begin(), files.end());

	return files;
}

std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    vector<string> files = rcFiles(nameInConfdir, nameInEnv);

    // Read all the contents
    std::string res;
    for (const auto& file: files)
        res += sys::read_file(file);
    return res;
}

SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    vector<string> files = rcFiles(nameInConfdir, nameInEnv);
    SourceCode res;

    // Read all the contents
    for (const auto& file: files)
    {
        string tmp = sys::read_file(file);
        res.push_back(FileInfo(file, tmp.size()));
        res.code += tmp;
    }
    return res;
}

}
}
