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

namespace wibble {
namespace str {

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

}
}

// vim:set ts=4 sw=4:
#endif
