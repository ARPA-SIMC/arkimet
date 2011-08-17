/*
 * intern - String interning
 *
 * Copyright (C) 2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/intern.h>
#include <vector>
#include <set>
#include <fstream>
#include <stdint.h>

using namespace std;

namespace arki {
namespace utils {

static const unsigned TABLE_SIZE = 1024;

StringInternTable::StringInternTable(size_t table_size)
    : table_size(table_size), table(new Bucket*[table_size])
{
    for (unsigned i = 0; i < TABLE_SIZE; ++i)
        table[i] = 0;
}

StringInternTable::~StringInternTable()
{
    for (unsigned i = 0; i < TABLE_SIZE; ++i)
        if (table[i])
            delete table[i];
    delete[] table;
}

// Bernstein hash function
unsigned StringInternTable::hash(const char *str)
{
    if (str == NULL) return 0;

    uint32_t h = 5381;

    for (const char *s = str; *s; ++s)
        h = (h * 33) ^ *s;

    return h % table_size;
}

// Bernstein hash function
unsigned StringInternTable::hash(const std::string& str)
{
    uint32_t h = 5381;
    for (string::const_iterator s = str.begin(); s != str.end(); ++s)
        h = (h * 33) ^ *s;

    return h % table_size;
}

}
}
