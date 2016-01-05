#include "configfile.h"
#include "config.h"
#include "exceptions.h"
#include "utils/string.h"
#include <arki/wibble/regexp.h>
#include <cctype>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace arki::utils;

namespace arki {

std::string ConfigFileParseError::describe(const std::string& filename, int line, const std::string& error)
{
    stringstream msg;
    msg << filename << ":" << line << ":" << error;
    return msg.str();
}

ConfigFile::ConfigFile()
{
}

ConfigFile::ConfigFile(const ConfigFile& cfg)
{
    merge(cfg);
}

ConfigFile::~ConfigFile()
{
	for (std::map<string, ConfigFile*>::iterator i = sections.begin();
			i != sections.end(); ++i)
		delete i->second;
}

ConfigFile& ConfigFile::operator=(const ConfigFile& cfg)
{
    // Handle self assignment
    if (&cfg == this)
        return *this;

    clear();
    merge(cfg);
    return *this;
}

void ConfigFile::clear()
{
    m_values.clear();
    values_pos.clear();
    for (std::map<string, ConfigFile*>::iterator i = sections.begin();
            i != sections.end(); ++i)
        delete i->second;
    sections.clear();
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
		value = str::strip(value);
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
            throw_system_error(fileName + ": cannot read one line");
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

void ConfigFile::parse(const std::string& in, const std::string& file_name)
{
    stringstream ss(in);
    parse(ss, file_name);
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

void ConfigFile::setValue(const std::string& key, int value)
{
    std::stringstream ss;
    ss << value;
    setValue(key, ss.str());
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
        string name = str::joinpath(secName, i->first);
        out << endl;
        out << "[" << name << "]" << endl;
        i->second->output(out, fileName, name);
    }
}

std::string ConfigFile::serialize() const
{
    stringstream ss;
    output(ss, "(memory)");
    return ss.str();
}

bool ConfigFile::boolValue(const std::string& str, bool def)
{
    if (str.empty())
        return def;
    string l = str::lower(str::strip(str));
	if (l.empty())
		return false;
	if (l == "true" || l == "yes" || l == "on" || l == "1")
		return true;
	if (l == "false" || l == "no" || l == "off" || l == "0")
		return false;
    throw std::runtime_error("cannot parse bool value: value \"" + str + "\" is not supported");
}

}
