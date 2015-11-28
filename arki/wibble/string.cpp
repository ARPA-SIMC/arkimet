/*
 * OO wrapper for regular expression functions
 *
 * Copyright (C) 2003--2008  Enrico Zini <enrico@debian.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/wibble/string.h>
#include <arki/wibble/exception.h>
#include <stack>
#include <cstdio>
#include <cstdlib>
#include <cstdarg>

using namespace std;

namespace wibble {
namespace str {

#if _WIN32 || __xlC__
int vasprintf (char **result, const char *format, va_list args)
{
  const char *p = format;
  /* Add one to make sure that it is never zero, which might cause malloc
     to return NULL.  */
  int total_width = strlen (format) + 1;
  va_list ap;

  memcpy ((void *)&ap, (void *)&args, sizeof (va_list));

  while (*p != '\0')
    {
      if (*p++ == '%')
	{
	  while (strchr ("-+ #0", *p))
	    ++p;
	  if (*p == '*')
	    {
	      ++p;
	      total_width += abs (va_arg (ap, int));
	    }
	  else
	    total_width += strtoul (p, (char **) &p, 10);
	  if (*p == '.')
	    {
	      ++p;
	      if (*p == '*')
		{
		  ++p;
		  total_width += abs (va_arg (ap, int));
		}
	      else
	      total_width += strtoul (p, (char **) &p, 10);
	    }
	  while (strchr ("hlL", *p))
	    ++p;
	  /* Should be big enough for any format specifier except %s and floats.  */
	  total_width += 30;
	  switch (*p)
	    {
	    case 'd':
	    case 'i':
	    case 'o':
	    case 'u':
	    case 'x':
	    case 'X':
	    case 'c':
	      (void) va_arg (ap, int);
	      break;
	    case 'f':
	    case 'e':
	    case 'E':
	    case 'g':
	    case 'G':
	      (void) va_arg (ap, double);
	      /* Since an ieee double can have an exponent of 307, we'll
		 make the buffer wide enough to cover the gross case. */
	      total_width += 307;
	      break;
	    case 's':
	      total_width += strlen (va_arg (ap, char *));
	      break;
	    case 'p':
	    case 'n':
	      (void) va_arg (ap, char *);
	      break;
	    }
	  p++;
	}
    }
  *result = (char*)malloc (total_width);
  if (*result != NULL) {
    return vsprintf (*result, format, args);}
  else {
    return 0;
  }
}
#endif

std::string fmtf( const char* f, ... ) {
    char *c;
    va_list ap;
    va_start( ap, f );
    vasprintf( &c, f, ap );
    std::string ret( c );
    free( c );
    return ret;
}

std::string fmt( const char* f, ... ) {
    char *c;
    va_list ap;
    va_start( ap, f );
    vasprintf( &c, f, ap );
    std::string ret( c );
    free( c );
    return ret;
}

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
	while (getline(*in, line))
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
		throw wibble::exception::Consistency("parsing yaml line \"" + line + "\"",
				"field continuation found, but no field has started");

	// Field start
	size_t pos = line.find(':');
	if (pos == string::npos)
		throw wibble::exception::Consistency("parsing Yaml line \"" + line + "\"",
				"every line that does not start with spaces must have a semicolon");

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

std::string c_escape(const std::string& str)
{
    string res;
    for (string::const_iterator i = str.begin(); i != str.end(); ++i)
        if (*i == '\n')
            res += "\\n";
        else if (*i == '\t')
            res += "\\t";
        else if (*i == 0 || iscntrl(*i))
        {
            char buf[5];
            snprintf(buf, 5, "\\x%02x", (unsigned int)*i);
            res += buf;
        }
        else if (*i == '"' || *i == '\\')
        {
            res += "\\";
            res += *i;
        }
        else
            res += *i;
    return res;
}

std::string c_unescape(const std::string& str, size_t& lenParsed)
{
    string res;
    string::const_iterator i = str.begin();
    for ( ; i != str.end() && *i != '"'; ++i)
        if (*i == '\\' && (i+1) != str.end())
        {
            switch (*(i+1))
            {
                case 'n': res += '\n'; break;
                case 't': res += '\t'; break;
                case 'x': {
                              size_t j;
                              char buf[5] = "0x\0\0";
                              // Read up to 2 extra hex digits
                              for (j = 0; j < 2 && i+2+j != str.end() && isxdigit(*(i+2+j)); ++j)
                                  buf[2+j] = *(i+2+j);
                              i += j;
                              res += (char)atoi(buf);
                              break;
                          }
                default:
                          res += *(i+1);
                          break;
            }
            ++i;
        } else
            res += *i;
    if (i != str.end() && *i == '"')
        ++i;
    lenParsed = i - str.begin();
    return res;
}


}
}

// vim:set ts=4 sw=4:
