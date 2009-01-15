#ifndef ARKI_TYPES_UTILS_H
#define ARKI_TYPES_UTILS_H

/*
 * types/utils - Utility code for implementing arkimet types
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <cctype>

namespace arki {
namespace types {

// Parse the outer style of a TYPE(val1, val2...) string
template<typename T>
static typename T::Style outerParse(const std::string& str, std::string& inner)
{
	using namespace std;

	if (str.empty())
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "string is empty");
	size_t pos = str.find('(');
	if (pos == string::npos)
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "no open parentesis found in \""+str+"\"");
	if (str[str.size() - 1] != ')')
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "string \""+str+"\" does not end with closed parentesis");
	inner = str.substr(pos+1, str.size() - pos - 2);
	return T::parseStyle(str.substr(0, pos));
}

// Parse a list of numbers of the given size
template<int SIZE>
struct NumberList
{
	int vals[SIZE];

	unsigned size() const { return SIZE; }

	NumberList(const std::string& str, const std::string& what)
	{
		using namespace std;
		using namespace wibble::str;

		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			if (!*start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"found " + fmt(i) + " values instead of " + fmt(SIZE));

			// Parse the number
			char* endptr;
			vals[i] = strtol(start, &endptr, 10);
			if (endptr == start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"expected a number, but found \"" + str.substr(start - str.c_str()) + "\"");

			start = endptr;
		}
		if (*start)
			throw wibble::exception::Consistency(string("parsing ") + what,
				"found trailing characters at the end: \"" + str.substr(start - str.c_str()) + "\"");
	}
};

// Parse a list of floats of the given size
template<int SIZE>
struct FloatList
{
	float vals[SIZE];

	unsigned size() const { return SIZE; }

	FloatList(const std::string& str, const std::string& what)
	{
		using namespace std;
		using namespace wibble::str;

		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			if (!*start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"found " + fmt(i) + " values instead of " + fmt(SIZE));

			// Parse the number
			char* endptr;
			vals[i] = strtof(start, &endptr);
			if (endptr == start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"expected a number, but found \"" + str.substr(start - str.c_str()) + "\"");

			start = endptr;
		}
		if (*start)
			throw wibble::exception::Consistency(string("parsing ") + what,
				"found trailing characters at the end: \"" + str.substr(start - str.c_str()) + "\"");
	}
};

}
}
// vim:set ts=4 sw=4:
#endif
