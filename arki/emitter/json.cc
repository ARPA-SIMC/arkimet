/*
 * emitter/json - JSON emitter
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/emitter/json.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <cctype>
#include <cmath>

using namespace std;
using namespace wibble;

namespace arki {
namespace emitter {

JSON::JSON(std::ostream& out) : out(out) {}
JSON::~JSON() {}

void JSON::val_head()
{
    if (!stack.empty())
    {
        switch (stack.back())
        {
            case LIST_FIRST: stack.back() = LIST; break;
            case LIST: out << ","; break;
            case MAPPING_KEY_FIRST: stack.back() = MAPPING_VAL; break;
            case MAPPING_KEY: out << ","; stack.back() = MAPPING_VAL; break;
            case MAPPING_VAL: out << ":"; stack.back() = MAPPING_KEY; break;
        }
    }
}

void JSON::add_null()
{
    val_head();

    out << "null";
}

void JSON::add_bool(bool val)
{
    val_head();

    if (val)
        out << "true";
    else
        out << "false";
}

void JSON::add_int(long long int val)
{
    val_head();
    out << val;
}

void JSON::add_double(double val)
{
    val_head();

    double vint, vfrac;
    vfrac = modf(val, &vint);
    if (vfrac == 0.0)
        out << (int)vint << ".0";
    else
        out << val;
}

void JSON::add_string(const std::string& val)
{
    val_head();
    out << '"';
    for (string::const_iterator i = val.begin(); i != val.end(); ++i)
        switch (*i)
        {
            case '"': out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '/': out << "\\/"; break;
            case '\b': out << "\\b"; break;
            case '\f': out << "\\f"; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default: out << *i; break;
        }
    out << '"';
}

void JSON::start_list()
{
    val_head();
    out << "[";
    stack.push_back(LIST_FIRST);
}

void JSON::end_list()
{
    out << "]";
    stack.pop_back();
}

void JSON::start_mapping()
{
    val_head();
    out << "{";
    stack.push_back(MAPPING_KEY_FIRST);
}

void JSON::end_mapping()
{
    out << "}";
    stack.pop_back();
}

struct JSONParseException : public wibble::exception::Consistency
{
    JSONParseException(const std::string& msg)
        : wibble::exception::Consistency("parsing JSON", msg) {}

    virtual const char* type() const throw () { return "JSONParse"; }
};

static void parse_spaces(std::istream& in)
{
    while (isspace(in.peek()))
        in.get();
}

static void parse_fixed(std::istream& in, const char* expected)
{
    const char* s = expected;
    while (*s)
    {
        int c = in.get();
        if (c != *s)
        {
            if (c == EOF)
                throw JSONParseException(str::fmtf("end of file reached looking for %s in %s", s, expected));
            else
                throw JSONParseException(str::fmtf("unexpected character '%c' looking for %s in %s", c, s, expected));
        }
        ++s;
    }
}

static void parse_number(std::istream& in, Emitter& e)
{
    string num;
    bool done = false;
    bool is_double = false;
    while (!done)
    {
        int c = in.peek();
        switch (c)
        {
            case '-':
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                num.append(1, in.get());
                break;
            case '.':
            case 'e':
            case 'E':
            case '+':
                is_double = true;
                num.append(1, in.get());
                break;
            default:
                done = true;
        }
    }

    if (is_double)
    {
        e.add_double(strtod(num.c_str(), NULL));
    } else {
        e.add_int(strtoll(num.c_str(), NULL, 10));
    }

    parse_spaces(in);
}

static void parse_string(std::istream& in, Emitter& e)
{
    string res;
    in.get(); // Eat the leading '"'
    bool done = false;
    while (!done)
    {
        int c = in.get();
        switch (c)
        {
            case '\\':
                c = in.get();
                if (c == EOF)
                    throw JSONParseException("unterminated string");
                switch (c)
                {
                    case 'b': res.append(1, '\b'); break;
                    case 'f': res.append(1, '\f'); break;
                    case 'n': res.append(1, '\n'); break;
                    case 'r': res.append(1, '\r'); break;
                    case 't': res.append(1, '\t'); break;
                    default: res.append(1, c); break;
                }
                break;
            case '"':
                done = true;
                break;
            case EOF:
                throw JSONParseException("unterminated string");
            default:
                res.append(1, c);
                break;
        }
    }
    parse_spaces(in);
    e.add(res);
}

static void parse_value(std::istream& in, Emitter& e);

static void parse_array(std::istream& in, Emitter& e)
{
    e.start_list();
    in.get(); // Eat the leading '['
    parse_spaces(in);
    while (in.peek() != ']')
    {
        parse_value(in, e);
        if (in.peek() == ',')
            in.get();
        parse_spaces(in);
    }
    in.get(); // Eat the trailing ']'
    parse_spaces(in);
    e.end_list();
}

static void parse_object(std::istream& in, Emitter& e)
{
    e.start_mapping();
    in.get(); // Eat the leading '{'
    parse_spaces(in);
    while (in.peek() != '}')
    {
        if (in.peek() != '"')
            throw JSONParseException("expected a string as object key");
        parse_string(in, e);
        parse_spaces(in);
        if (in.peek() == ':')
            in.get();
        else
            throw JSONParseException("':' expected after object key");
        parse_value(in, e);
        if (in.peek() == ',')
            in.get();
        parse_spaces(in);
    }
    in.get(); // Eat the trailing '}'
    parse_spaces(in);
    e.end_mapping();
}

static void parse_value(std::istream& in, Emitter& e)
{
    parse_spaces(in);
    switch (in.peek())
    {
        case EOF:
            throw JSONParseException("JSON string is truncated");
        case '{': parse_object(in, e); break;
        case '[': parse_array(in, e); break;
        case '"': parse_string(in, e); break;
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9': parse_number(in, e); break;
        case 't':
            parse_fixed(in, "true");
            e.add(true);
            parse_spaces(in);
            break;
        case 'f':
            parse_fixed(in, "false");
            e.add(false);
            parse_spaces(in);
            break;
        case 'n':
            parse_fixed(in, "null");
            e.add_null();
            parse_spaces(in);
            break;
        default:
            throw JSONParseException(str::fmtf("unexpected character '%c'", in.peek()));
    }
    parse_spaces(in);
}

void JSON::parse(std::istream& in, Emitter& e)
{
    parse_value(in, e);
}

}
}

// vim:set ts=4 sw=4:
