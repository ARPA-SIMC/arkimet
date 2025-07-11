#include "cfg.h"
#include "arki/utils/regexp.h"
#include "arki/utils/string.h"
#include "file.h"
#include <cstdio>
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace core {
namespace cfg {

std::string ParseError::describe(const std::string& filename, int line,
                                 const std::string& error)
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
          assignment("^[ \t]*([a-zA-Z0-9_.-]+([ \t]+[a-zA-Z0-9_.-]+)*)[ \t]*=[ "
                     "\t]*(.*)$",
                     4)
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

class SectionParser : public ParserBase
{
public:
    using ParserBase::ParserBase;

    /**
     * Parse configuration from the given input stream.
     *
     * Finding the beginning of a new section will raise an error
     *
     * Returns true if a new section follows, false if it stops at the end of
     * file.
     */
    std::shared_ptr<Section> parse_section()
    {
        auto dest = std::make_shared<Section>();

        while (readline())
        {
            if (patterns.sec_start.match(line))
                throw_parse_error("[section] line found in a config file that "
                                  "should not contain sections");

            if (patterns.empty_line.match(line))
                continue;

            if (patterns.assignment.match(line))
            {
                string value = patterns.assignment[3];
                // Strip leading and trailing spaces on the value
                value        = str::strip(value);
                // Strip double quotes, if they appear
                if (value[0] == '"' && value[value.size() - 1] == '"')
                    value = value.substr(1, value.size() - 2);
                dest->set(patterns.assignment[1], value);
            }
            else
                throw_parse_error("line is not a comment, nor a section start, "
                                  "nor empty, nor a key=value assignment");
        }

        return dest;
    }

    std::shared_ptr<Sections> parse_sections()
    {
        auto dest = std::make_shared<Sections>();
        std::shared_ptr<Section> section;

        while (readline())
        {
            if (patterns.empty_line.match(line))
                continue;

            if (section)
            {
                if (patterns.sec_start.match(line))
                    section = dest->obtain(patterns.sec_start[1]);
                else if (patterns.assignment.match(line))
                {
                    std::string value = patterns.assignment[3];
                    // Strip leading and trailing spaces on the value
                    value             = str::strip(value);
                    // Strip double quotes, if they appear
                    if (value[0] == '"' && value[value.size() - 1] == '"')
                        value = value.substr(1, value.size() - 2);
                    section->set(patterns.assignment[1], value);
                }
                else
                    throw_parse_error(
                        "line is not a comment, nor a section start, nor "
                        "empty, nor a key=value assignment");
            }
            else
            {
                if (patterns.sec_start.match(line))
                    section = dest->obtain(patterns.sec_start[1]);
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

bool Section::has(const std::string& key) const { return find(key) != end(); }

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
    throw std::runtime_error("cannot parse bool value for key \"" + key +
                             "\": value \"" + i->second +
                             "\" is not supported");
}

void Section::set(const std::string& key, const std::string& value)
{
    (*this)[key] = value;
}

void Section::set(const std::string& key, int value)
{
    set(key, std::to_string(value));
}

void Section::unset(const std::string& key) { erase(key); }

void Section::write(std::ostream& out) const
{
    for (const auto& i : *this)
        if (!i.second.empty())
            out << i.first << " = " << i.second << endl;
}

std::string Section::to_string() const
{
    std::stringstream buf;
    write(buf);
    return buf.str();
}

void Section::dump(FILE* out) const
{
    for (const auto& i : *this)
        if (!i.second.empty())
            fprintf(out, "%s = %s\n", i.first.c_str(), i.second.c_str());
}

std::shared_ptr<Section> Section::parse(core::NamedFileDescriptor& in)
{
    auto reader = LineReader::from_fd(in);
    return parse(*reader, in.path().native());
}

std::shared_ptr<Section> Section::parse(const std::string& in,
                                        const std::string& pathname)
{
    auto reader = LineReader::from_chars(in.data(), in.size());
    return parse(*reader, pathname);
}

std::shared_ptr<Section> Section::parse(core::LineReader& in,
                                        const std::string& pathname)
{
    SectionParser parser(pathname, in);
    return parser.parse_section();
}

/*
 * Sections
 */

Sections::Sections(const Sections& o) : Sections()
{
    for (const auto& i : o)
        emplace(i.first, std::make_shared<Section>(*i.second));
}

Sections& Sections::operator=(const Sections& o)
{
    if (this == &o)
        return *this;
    clear();
    for (const auto& i : o)
        emplace(i.first, std::make_shared<Section>(*i.second));
    return *this;
}

std::shared_ptr<const Section> Sections::section(const std::string& key) const
{
    auto i = find(key);
    if (i == end())
        return nullptr;
    return i->second;
}

std::shared_ptr<Section> Sections::section(const std::string& key)
{
    auto i = find(key);
    if (i == end())
        return nullptr;
    return i->second;
}

std::shared_ptr<Section> Sections::obtain(const std::string& name)
{
    iterator i = find(name);
    if (i != end())
        return i->second;

    auto r = emplace(name, std::make_shared<Section>());
    return r.first->second;
}

std::string Sections::to_string() const
{
    std::stringstream buf;
    write(buf);
    return buf.str();
}

void Sections::write(std::ostream& out) const
{
    bool first = true;
    for (const auto& si : *this)
    {
        if (first)
            first = false;
        else
            out << endl;

        out << "[" << si.first << "]" << endl;
        si.second->write(out);
    }
}

void Sections::dump(FILE* out) const
{
    for (const auto& i : *this)
    {
        fprintf(out, "[%s]\n", i.first.c_str());
        i.second->dump(out);
    }
}

std::shared_ptr<Sections> Sections::parse(core::NamedFileDescriptor& in)
{
    auto reader = LineReader::from_fd(in);
    return parse(*reader, in.path());
}

std::shared_ptr<Sections> Sections::parse(const std::string& in,
                                          const std::string& pathname)
{
    auto reader = LineReader::from_chars(in.data(), in.size());
    return parse(*reader, pathname);
}

std::shared_ptr<Sections> Sections::parse(core::LineReader& in,
                                          const std::string& pathname)
{
    SectionParser parser(pathname, in);
    return parser.parse_sections();
}

} // namespace cfg
} // namespace core
} // namespace arki
