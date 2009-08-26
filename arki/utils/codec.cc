/*
 * utils/codec - Encoding/decoding utilities
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/codec.h>

using namespace std;

namespace arki {
namespace utils {
namespace codec {

uint64_t decodeULInt(const unsigned char* val, unsigned int bytes)
{
	uint64_t res = 0;
	for (unsigned int i = 0; i < bytes; ++i)
	{
		res <<= 8;
		res |= val[i];
	}
	return res;
}

}
}
}
// vim:set ts=4 sw=4:
