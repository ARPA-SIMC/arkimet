#include "cfg.h"
#include "file.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include <cctype>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace core {
namespace cfg {

std::string ParseError::describe(const std::string& filename, int line, const std::string& error)
{
    stringstream msg;
    msg << filename << ":" << line << ":" << error;
    return msg.str();
}

struct Patterns
{
    ERegexp sec_start;
    ERegexp empty_line;
    ERegexp assignment;

    Patterns()
        : sec_start("^\\[[ \t]*([a-zA-Z0-9_.-]+)[ \t]*\\]", 2),
          empty_line("^[ \t]*([#;].*)?$"),
          assignment("^[ \t]*([a-zA-Z0-9_.-]+([ \t]+[a-zA-Z0-9_.-]+)*)[ \t]*=[ \t]*(.*)$", 4)
    {
    }
};


struct ParserBase
{
    Patterns patterns;
    LineReader& reader;
    std::string pathname;
    std::string line;
    int lineno = 0;

    ParserBase(const std::string& pathname, LineReader& reader)
        : reader(reader), pathname(pathname)
    {
    }

    bool eof() const { return reader.eof(); }

    bool readline()
    {
        bool res = reader.getline(line);
        ++lineno;
        return res;
    }

    [[noreturn]] void throw_parse_error(const std::string& msg) const
    {
        throw ParseError(pathname, lineno, msg);
    }

};


struct SectionParser : public ParserBase
{
    using ParserBase::ParserBase;

    /**
     * Parse configuration from the given input stream.
     *
     * Finding the beginning of a new section will raise an error
     *
     * Returns true if a new section follows, false if it stops at the end of
     * file.
     */
    Section parse_section()
    {
        Section dest;

        while (readline())
        {
            if (patterns.sec_start.match(line))
                throw_parse_error("[section] line found in a config file that should not contain sections");

            if (patterns.empty_line.match(line))
                continue;

            if (patterns.assignment.match(line))
            {
                string value = patterns.assignment[3];
                // Strip leading and trailing spaces on the value
                value = str::strip(value);
                // Strip double quotes, if they appear
                if (value[0] == '"' && value[value.size()-1] == '"')
                    value = value.substr(1, value.size()-2);
                dest.set(patterns.assignment[1], value);
            }
            else
                throw_parse_error("line is not a comment, nor a section start, nor empty, nor a key=value assignment");
        }

        return dest;
    }

    Sections parse_sections()
    {
        Sections dest;
        Section* section = nullptr;

        while (readline())
        {
            if (patterns.empty_line.match(line))
                continue;

            if (section)
            {
                if (patterns.sec_start.match(line))
                    section = &dest.obtain(patterns.sec_start[1]);
                else if (patterns.assignment.match(line)) {
                    std::string value = patterns.assignment[3];
                    // Strip leading and trailing spaces on the value
                    value = str::strip(value);
                    // Strip double quotes, if they appear
                    if (value[0] == '"' && value[value.size()-1] == '"')
                        value = value.substr(1, value.size()-2);
                    section->set(patterns.assignment[1], value);
                }
                else
                    throw_parse_error("line is not a comment, nor a section start, nor empty, nor a key=value assignment");
            } else {
                if (patterns.sec_start.match(line))
                    section = &dest.obtain(patterns.sec_start[1]);
                else
                    throw_parse_error("expected a [section] start");
            }
        }

        return dest;
    }
};


/*
 * Section
 */

bool Section::has(const std::string& key) const
{
    return find(key) != end();
}

std::string Section::value(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        return std::string();
    return i->second;
}

bool Section::value_bool(const std::string& key, bool def) const
{
    const_iterator i = find(key);
    if (i == end())
        return def;

    std::string l = str::lower(str::strip(i->second));
    if (l.empty())
        return false;
    if (l == "true" || l == "yes" || l == "on" || l == "1")
        return true;
    if (l == "false" || l == "no" || l == "off" || l == "0")
        return false;
    throw std::runtime_error("cannot parse bool value for key \"" + key + "\": value \"" + i->second + "\" is not supported");
}

void Section::set(const std::string& key, const std::string& value)
{
    (*this)[key] = value;
}

void Section::set(const std::string& key, int value)
{
    set(key, std::to_string(value));
}

void Section::write(std::ostream& out, const std::string& pathname) const
{
    for (const auto& i: *this)
        if (!i.second.empty())
            out << i.first << " = " << i.second << endl;
}

Section Section::parse(core::NamedFileDescriptor& in)
{
    auto reader = LineReader::from_fd(in);
    return parse(*reader, in.name());
}

Section Section::parse(const std::string& in, const std::string& pathname)
{
    auto reader = LineReader::from_chars(in.data(), in.size());
    return parse(*reader, pathname);
}

Section Section::parse(core::LineReader& in, const std::string& pathname)
{
    SectionParser parser(pathname, in);
    return parser.parse_section();
}


/*
 * Sections
 */

const Section* Sections::section(const std::string& key) const
{
    auto i = find(key);
    if (i == end())
        return nullptr;
    return &(i->second);
}

Section* Sections::section(const std::string& key)
{
    auto i = find(key);
    if (i == end())
        return nullptr;
    return &(i->second);
}

Section& Sections::obtain(const std::string& name)
{
    iterator i = find(name);
    if (i != end())
        return i->second;

    auto r = emplace(name, Section());
    return r.first->second;
}

void Sections::write(std::ostream& out, const std::string& pathname) const
{
    bool first = true;
    for (const auto& si: *this)
    {
        if (first)
            first = false;
        else
            out << endl;

        out << "[" << si.first << "]" << endl;
        si.second.write(out, pathname);
    }
}

Sections Sections::parse(core::NamedFileDescriptor& in)
{
    auto reader = LineReader::from_fd(in);
    return parse(*reader, in.name());
}

Sections Sections::parse(const std::string& in, const std::string& pathname)
{
    auto reader = LineReader::from_chars(in.data(), in.size());
    return parse(*reader, pathname);
}

Sections Sections::parse(core::LineReader& in, const std::string& pathname)
{
    SectionParser parser(pathname, in);
    return parser.parse_sections();
}

}
}
}
