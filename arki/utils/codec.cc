/*
 * utils/codec - Encoding/decoding utilities
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/utils/codec.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;

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

Encoder& Encoder::addBuffer(const wibble::sys::Buffer& buf)
{
    this->buf.append((const char*)buf.data(), buf.size());
    return *this;
}

Decoder::Decoder(const wibble::sys::Buffer& buf, size_t offset)
    : buf((const unsigned char*)buf.data() + offset), len(buf.size() - offset)
{
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
                              char buf[5] = "0x\0\0";
                              // Read up to 2 extra hex digits
                              for (size_t j = 0; j < 2 && i+2+j != str.end() && isxdigit(*(i+2+j)); ++j)
                                  buf[2+j] = *(i+2+j);
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
}
// vim:set ts=4 sw=4:
