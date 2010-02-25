/*
 * runtime/config - Common configuration-related code used in most arkimet executables
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/runtime/config.h>
#include <arki/dataset.h>
#include <arki/utils.h>
#include <arki/matcher.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>
#include <fstream>
#include <memory>

using namespace std;
using namespace wibble;
using namespace wibble::commandline;

namespace arki {
namespace runtime {

void parseConfigFile(ConfigFile& cfg, const std::string& fileName)
{
	using namespace wibble;

	if (fileName == "-")
	{
		// Parse the config file from stdin
		cfg.parse(cin, fileName);
		return;
	}

	// Remove trailing slashes, if any
	string fname = fileName;
	while (!fname.empty() && fname[fname.size() - 1] == '/')
		fname.resize(fname.size() - 1);

	// Check if it's a file or a directory
	std::auto_ptr<struct stat> st = sys::fs::stat(fname);
	if (st.get() == 0)
		throw wibble::exception::Consistency("reading configuration from " + fname, fname + " does not exist");
	if (S_ISDIR(st->st_mode))
	{
		// If it's a directory, merge in its config file
		string name = str::basename(fname);
		string file = str::joinpath(fname, "config");

		ConfigFile section;
		ifstream in;
		in.open(file.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(file, "opening config file for reading");
		// Parse the config file into a new section
		section.parse(in, file);
		// Fill in missing bits
		section.setValue("name", name);
		section.setValue("path", sys::fs::abspath(fname));
		// Merge into cfg
		cfg.mergeInto(name, section);
	} else {
		// If it's a file, then it's a merged config file
		ifstream in;
		in.open(fname.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(fname, "opening config file for reading");
		// Parse the config file
		cfg.parse(in, fname);
	}
}

bool parseConfigFiles(ConfigFile& cfg, wibble::commandline::Parser& opts)
{
	bool found = false;
	while (opts.hasNext())
	{
		ReadonlyDataset::readConfig(opts.next(), cfg);
		found = true;
	}
	return found;
}

bool parseConfigFiles(ConfigFile& cfg, const wibble::commandline::VectorOption<wibble::commandline::String>& files)
{
       bool found = false;
       for (vector<string>::const_iterator i = files.values().begin();
                       i != files.values().end(); ++i)
       {
               parseConfigFile(cfg, *i);
               //ReadonlyDataset::readConfig(*i, cfg);
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

void readMatcherAliasDatabase(wibble::commandline::StringOption* file)
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
	auto_ptr<struct stat> st = wibble::sys::fs::stat(name);
	if (st.get())
	{
		parseConfigFile(cfg, name);
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}
#endif

	// Else, nothing is loaded.
}

std::string rcDirName(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir)
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

std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	std::string dirname = rcDirName(nameInConfdir, nameInEnv, dirOption);

	vector<string> files;
	wibble::sys::fs::Directory dir(dirname);
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		string file = *i;
		// Skip hidden files
		if (file[0] == '.') continue;
		// Skip backup files
		if (file[file.size() - 1] == '~') continue;
		// Skip non-files
		if (i->d_type == DT_UNKNOWN)
		{
			std::auto_ptr<struct stat> st = wibble::sys::fs::stat(dirname + "/" + file);
			if (!S_ISREG(st->st_mode)) continue;
		} else if (i->d_type != DT_REG)
			continue;
		files.push_back(wibble::str::joinpath(dirname, *i));
	}

	// Sort the file names
	std::sort(files.begin(), files.end());

	return files;
}

std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);

	// Read all the contents
	string res;
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
		res += utils::readFile(*i);
	return res;
}

SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);
	SourceCode res;

	// Read all the contents
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		string tmp = utils::readFile(*i);
		res.push_back(FileInfo(*i, tmp.size()));
		res.code += tmp;
	}
	return res;
}

}
}
// vim:set ts=4 sw=4:
