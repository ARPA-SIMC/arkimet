// -*- C++ -*-
#ifndef WIBBLE_STRING_H
#define WIBBLE_STRING_H

/*
 * Various string functions
 *
 * Copyright (C) 2007,2008  Enrico Zini <enrico@debian.org>
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

#include <cstdarg>
#include <cstdio>
#include <string>
#include <set>
#include <vector>
#include <deque>
#include <sstream>
#include <cctype>
#ifdef _WIN32
#include <cstring>
#include <cstdlib>
#endif

namespace wibble {
namespace str {

#ifdef _WIN32
static int vasprintf (char **, const char *, va_list);
#endif

std::string fmtf( const char* f, ... );
template< typename T > inline std::string fmt(const T& val);

/// Format any value into a string using a std::stringstream
template< typename T >
inline std::string fmt(const T& val)
{
    std::stringstream str;
    str << val;
    return str.str();
}

template<> inline std::string fmt<std::string>(const std::string& val) {
    return val;
}
template<> inline std::string fmt<char*>(char * const & val) { return val; }

/// Check if a string starts with the given substring
inline bool startsWith(const std::string& str, const std::string& part)
{
	if (str.size() < part.size())
		return false;
	return str.substr(0, part.size()) == part;
}

/// Check if a string ends with the given substring
inline bool endsWith(const std::string& str, const std::string& part)
{
	if (str.size() < part.size())
		return false;
	return str.substr(str.size() - part.size()) == part;
}

/**
 * Parse a record of Yaml-style field: value couples.
 *
 * Parsing stops either at end of record (one empty line) or at end of file.
 *
 * The value is deindented properly.
 *
 * Example code:
 * \code
 *	utils::YamlStream stream;
 *	map<string, string> record;
 *	std::copy(stream.begin(inputstream), stream.end(), inserter(record));
 * \endcode
 */
class YamlStream
{
public:
	// TODO: add iterator_traits
	class const_iterator
	{
		std::istream* in;
		std::pair<std::string, std::string> value;
		std::string line;

	public:
		const_iterator(std::istream& in);
		const_iterator() : in(0) {}

		const_iterator& operator++();

		const std::pair<std::string, std::string>& operator*() const
		{
			return value;
		}
		const std::pair<std::string, std::string>* operator->() const
		{
			return &value;
		}
		bool operator==(const const_iterator& ti) const
		{
			return in == ti.in;
		}
		bool operator!=(const const_iterator& ti) const
		{
			return in != ti.in;
		}
	};

	const_iterator begin(std::istream& in) { return const_iterator(in); }
	const_iterator end() { return const_iterator(); }
};

/**
 * Escape the string so it can safely used as a C string inside double quotes
 */
std::string c_escape(const std::string& str);

/**
 * Unescape a C string, stopping at the first double quotes or at the end of
 * the string.
 *
 * lenParsed is set to the number of characters that were pased (which can be
 * greather than the size of the resulting string in case escapes were found)
 */
std::string c_unescape(const std::string& str, size_t& lenParsed);


}
}

// vim:set ts=4 sw=4:
#endif
