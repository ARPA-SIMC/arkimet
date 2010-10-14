/*
 * configfile - Read ini-style config files
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "configfile.h"

#include <wibble/exception.h>
#include <wibble/regexp.h>
#include <wibble/string.h>
#include <cctype>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace wibble;

namespace arki {

const char* ConfigFileParseError::type() const throw ()
{
	return "ConfigFileParseError";
}
std::string ConfigFileParseError::desc() const throw ()
{
	stringstream msg;
	msg << m_name << ":" << m_line << ":" << m_error;
	return msg.str();
}

ConfigFile::ConfigFile()
{
}

ConfigFile::~ConfigFile()
{
	for (std::map<string, ConfigFile*>::iterator i = sections.begin();
			i != sections.end(); ++i)
		delete i->second;
}

struct ConfigFileParserHelper
{
	wibble::ERegexp sec_start;
	wibble::ERegexp empty_line;
	wibble::ERegexp assignment;
	string fileName;
	int line;

	ConfigFileParserHelper(const std::string& fileName)
		: sec_start("^\\[[ \t]*([a-zA-Z0-9_.-]+)[ \t]*\\]", 2),
		  empty_line("^[ \t]*([#;].*)?$"),
		  assignment("^[ \t]*([a-zA-Z0-9_.-]+([ \t]+[a-zA-Z0-9_.-]+)*)[ \t]*=[ \t]*(.*)$", 4),
		  fileName(fileName),
		  line(1)
	{
	}
};

void ConfigFile::merge(const ConfigFile& c)
{
	// Copy the values and the values information
	for (const_iterator i = c.begin(); i != c.end(); ++i)
	{
		m_values.insert(*i);
		std::map<std::string, FilePos>::const_iterator j = c.values_pos.find(i->first);
		if (j == c.values_pos.end())
			values_pos.erase(i->first);
		else
			values_pos.insert(*j);
	}

	for (const_section_iterator i = c.sectionBegin(); i != c.sectionEnd(); ++i)
	{
		section_iterator sec = sections.find(i->first);
		if (sec == sections.end())
		{
			ConfigFile* nsec = new ConfigFile;
			pair<section_iterator, bool> res = sections.insert(make_pair(i->first, nsec));
			sec = res.first;
		}
		sec->second->merge(*i->second);
	}
}

void ConfigFile::mergeInto(const std::string& sectionName, const ConfigFile& c)
{
	// Access the section, creating it if it's missing
	std::map<string, ConfigFile*>::iterator i = sections.find(sectionName);
	ConfigFile* sec = 0;
	if (i == sections.end())
	{
		sec = new ConfigFile;
		sections.insert(make_pair(sectionName, sec));
	} else {
		sec = i->second;
	}

	// Merge c into the section
	sec->merge(c);
}

void ConfigFile::parseLine(ConfigFileParserHelper& h, const std::string& line)
{
	if (h.empty_line.match(line))
		return;
	if (h.assignment.match(line))
	{
		string value = h.assignment[3];
		// Strip leading and trailing spaces on the value
		value = str::trim(value);
		// Strip double quotes, if they appear
		if (value[0] == '"' && value[value.size()-1] == '"')
			value = value.substr(1, value.size()-2);
		m_values.insert(make_pair(h.assignment[1], value));
		values_pos.insert(make_pair(h.assignment[1], FilePos(h.fileName, h.line)));
	}
	else
		throw ConfigFileParseError(h.fileName, h.line, "line is not a comment, nor a section start, nor empty, nor a key=value assignment");
}

void ConfigFile::parse(std::istream& in, const std::string& fileName)
{
	ConfigFileParserHelper h(fileName);

	string line;
	while (!in.eof())
	{
		getline(in, line);
		if (in.fail() && !in.eof())
			throw wibble::exception::File(fileName, "reading one line");
		if (h.sec_start.match(line))
		{
			string name = h.sec_start[1];
			ConfigFile* c = new ConfigFile();
			c->parseSection(h, in);
			std::map<string, ConfigFile*>::iterator sec = sections.find(name);
			if (sec == sections.end())
			{
				sections.insert(make_pair(name, c));
			} else {
				sec->second->merge(*c);
				delete c;
			}
		} else {
			parseLine(h, line);
		}
		++h.line;
	}
}

void ConfigFile::parseSection(ConfigFileParserHelper& h, std::istream& in)
{
	string line;
	int c;
	while ((c = in.peek()) != EOF && c != '[')
	{
		getline(in, line);
		parseLine(h, line);
		++h.line;
	}
}


std::string ConfigFile::value(const std::string& key) const
{
	const_iterator i = m_values.find(key);
	if (i == m_values.end())
		return string();
	return i->second;
}

const ConfigFile::FilePos* ConfigFile::valueInfo(const std::string& key) const
{
	std::map<std::string, FilePos>::const_iterator i = values_pos.find(key);
	if (i == values_pos.end())
		return 0;
	return &(i->second);
}

void ConfigFile::setValue(const std::string& key, const std::string& value)
{
    m_values[key] = value;
	values_pos.erase(key);
}

ConfigFile* ConfigFile::section(const std::string& key) const
{
	const_section_iterator i = sections.find(key);
	if (i == sections.end())
		return 0;
	return i->second;
}

ConfigFile* ConfigFile::obtainSection(const std::string& name)
{
	const_section_iterator i = sections.find(name);
	if (i != sections.end())
		return i->second;

	ConfigFile* c;
	sections.insert(make_pair(name, c = new ConfigFile()));
	return c;
}

void ConfigFile::deleteSection(const std::string& key)
{
	std::map<std::string, ConfigFile*>::iterator i = sections.find(key);
	if (i != sections.end())
	{
		delete i->second;
		sections.erase(i);
	}
}

void ConfigFile::output(std::ostream& out, const std::string& fileName, const std::string& secName) const
{
	// First, the values
	for (const_iterator i = begin(); i != end(); ++i)
		if (!i->second.empty())
			out << i->first << " = " << i->second << endl;

	// Then, the sections
	for (const_section_iterator i = sectionBegin(); i != sectionEnd(); ++i)
	{
		string name = wibble::str::joinpath(secName, i->first);
		out << endl;
		out << "[" << name << "]" << endl;
		i->second->output(out, fileName, name);
	}
}

bool ConfigFile::boolValue(const std::string& str, bool def)
{
	if (str.empty())
		return def;
	string l = wibble::str::tolower(wibble::str::trim(str));
	if (l.empty())
		return false;
	if (l == "true" || l == "yes" || l == "on" || l == "1")
		return true;
	if (l == "false" || l == "no" || l == "off" || l == "0")
		return false;
	throw wibble::exception::Consistency("parsing bool value", "value \"" + str + "\" is not supported");
}


}
// vim:set ts=4 sw=4:
