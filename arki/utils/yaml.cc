#include "yaml.h"
#include <istream>

using namespace std;

namespace arki {
namespace utils {

static std::string stripYamlComment(const std::string& str)
{
    std::string res;
    for (string::const_iterator i = str.begin(); i != str.end(); ++i)
    {
        if (*i == '#')
            break;
        res += *i;
    }
    // Remove trailing spaces
    while (!res.empty() && ::isspace(res[res.size() - 1]))
        res.resize(res.size() - 1);
    return res;
}

YamlStream::const_iterator::const_iterator(std::istream& sin)
    : in(&sin)
{
    // Read the next line to parse, skipping leading empty lines
    while (std::getline(*in, line))
    {
        line = stripYamlComment(line);
        if (!line.empty())
            break;
    }

    if (line.empty() && in->eof())
        // If we reached EOF without reading anything, become the end iterator
        in = 0;
    else
        // Else do the parsing
        ++*this;
}

YamlStream::const_iterator& YamlStream::const_iterator::operator++()
{
    // Reset the data
    value.first.clear();
    value.second.clear();

    // If the lookahead line is empty, then we've reached the end of the
    // record, and we become the end iterator
    if (line.empty())
    {
        in = 0;
        return *this;
    }

    if (line[0] == ' ')
        throw std::runtime_error("cannot parse yaml line \"" + line + "\": field continuation found, but no field has started");

    // Field start
    size_t pos = line.find(':');
    if (pos == string::npos)
        throw std::runtime_error("parsing Yaml line \"" + line + "\": every line that does not start with spaces must have a semicolon");

    // Get the field name
    value.first = line.substr(0, pos);

    // Skip leading spaces in the value
    for (++pos; pos < line.size() && line[pos] == ' '; ++pos)
        ;

    // Get the (start of the) field value
    value.second = line.substr(pos);

    // Look for continuation lines, also preparing the lookahead line
    size_t indent = 0;
    while (true)
    {
        line.clear();
        if (in->eof()) break;
        if (!getline(*in, line)) break;
        // End of record
        if (line.empty()) break;
        // Full comment line: ignore it
        if (line[0] == '#') continue;
        // New field or empty line with comment
        if (line[0] != ' ')
        {
            line = stripYamlComment(line);
            break;
        }

        // Continuation line

        // See how much we are indented
        size_t this_indent;
        for (this_indent = 0; this_indent < line.size() && line[this_indent] == ' '; ++this_indent)
            ;

        if (indent == 0)
        {
            indent = this_indent;
            // If it's the first continuation line, and there was content right
            // after the field name, add a \n to it
            if (!value.second.empty())
                value.second += '\n';
        }

        if (this_indent > indent)
            // If we're indented the same or more than the first line, deindent
            // by the amount of indentation found in the first line
            value.second += line.substr(indent);
        else
            // Else, the line is indented less than the first line, just remove
            // all leading spaces.  Ugly, but it's been encoded in an ugly way.
            value.second += line.substr(this_indent);
        value.second += '\n';
    }

    return *this;
}

}
}
