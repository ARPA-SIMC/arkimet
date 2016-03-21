#include "arki/runtime/config.h"
#include "arki/runtime/io.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/matcher.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <algorithm>
#include <memory>
#include <unistd.h>

using namespace std;
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

    if (const char* envdir = getenv("ARKI_TMPDIR"))
        dir_temp = envdir;
    else if (const char* envdir = getenv("TMPDIR"))
        dir_temp = envdir;
    else
        dir_temp = "/tmp";

    if (const char* envurl = getenv("ARKI_INBOUND"))
        url_inbound = envurl;

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

    out << "URL of remote inbound directory: ";
    if (url_inbound.empty())
        out << "none." << endl;
    else
        out << url_inbound << endl;
    describe_envvar(out, "ARKI_INBOUND");

    out << "Temporary directory: " << dir_temp << "(set with ARKI_TMPDIR and TMPDIR)" << endl;

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

void parseConfigFile(ConfigFile& cfg, const std::string& fileName)
{
    if (fileName == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        cfg.parse(in);
        return;
    }

	// Remove trailing slashes, if any
	string fname = fileName;
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

        ConfigFile section;
        runtime::InputFile in(file);
        // Parse the config file into a new section
        section.parse(in);
        // Fill in missing bits
        section.setValue("name", name);
        section.setValue("path", sys::abspath(fname));
        // Merge into cfg
        cfg.mergeInto(name, section);
    } else {
        // If it's a file, then it's a merged config file
        runtime::InputFile in(fname);
        // Parse the config file
        cfg.parse(in);
    }
}

bool parseConfigFiles(ConfigFile& cfg, const commandline::VectorOption<commandline::String>& files)
{
       bool found = false;
       for (vector<string>::const_iterator i = files.values().begin();
                       i != files.values().end(); ++i)
       {
               parseConfigFile(cfg, *i);
               //Reader::readConfig(*i, cfg);
               found = true;
       }
       return found;
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

bool Restrict::is_allowed(const std::string& str)
{
	if (wanted.empty()) return true;
	return is_allowed(parseRestrict(str));

}
bool Restrict::is_allowed(const std::set<std::string>& names)
{
	if (wanted.empty()) return true;
	for (set<string>::const_iterator i = wanted.begin(); i != wanted.end(); ++i)
		if (names.find(*i) != names.end())
			return true;
	return false;
}
bool Restrict::is_allowed(const ConfigFile& cfg)
{
	if (wanted.empty()) return true;
	return is_allowed(parseRestrict(cfg.value("restrict")));
}
void Restrict::remove_unallowed(ConfigFile& cfg)
{
	vector<string> to_delete;
	for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
			i != cfg.sectionEnd(); ++i)
	{
		if (not is_allowed(*i->second))
			to_delete.push_back(i->first);
	}
	for (vector<string>::const_iterator i = to_delete.begin();
			i != to_delete.end(); ++i)
		cfg.deleteSection(*i);
}

void readMatcherAliasDatabase(commandline::StringOption* file)
{
	ConfigFile cfg;

	// The file named in the given StringOption (if any) is tried first.
	if (file && file->isSet())
	{
		parseConfigFile(cfg, file->stringValue());
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}

	// Otherwise the file given in the environment variable ARKI_ALIASES is tried.
	char* fromEnv = getenv("ARKI_ALIASES");
	if (fromEnv)
	{
		parseConfigFile(cfg, fromEnv);
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}

#ifdef CONF_DIR
    // Else, CONF_DIR is tried.
    string name = string(CONF_DIR) + "/match-alias.conf";
    unique_ptr<struct stat> st = sys::stat(name);
    if (st.get())
    {
        parseConfigFile(cfg, name);
        MatcherAliasDatabase::addGlobal(cfg);
        return;
    }
#endif

	// Else, nothing is loaded.
}

static std::string rcDirName(const std::string& nameInConfdir, const std::string& nameInEnv, commandline::StringOption* dir)
{
	std::string dirname;
	char* fromEnv = 0;

	// The directory named in the given StringOption (if any) is tried first.
	if (dir && dir->isSet())
		return dir->stringValue();
	
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

std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv, commandline::StringOption* dirOption)
{
	std::string dirname = rcDirName(nameInConfdir, nameInEnv, dirOption);

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

std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);

	// Read all the contents
	string res;
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
		res += files::read_file(*i);
	return res;
}

SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);
	SourceCode res;

	// Read all the contents
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		string tmp = files::read_file(*i);
		res.push_back(FileInfo(*i, tmp.size()));
		res.code += tmp;
	}
	return res;
}

}
}
