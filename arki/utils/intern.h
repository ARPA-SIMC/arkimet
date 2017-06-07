/*
 * intern - String interning
 *
 * Copyright (C) 2011  ARPAE-SIMC <simc-urp@arpae.it>
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

#ifndef ARKI_UTILS_INTERN_H
#define ARKI_UTILS_INTERN_H

/** @file
 * String interning
 */

#include <vector>
#include <string>

namespace arki {
namespace utils {

/**
 * Hashtable-based string interning
 */
struct StringInternTable
{
    struct Bucket
    {
        std::string val;
        int id;
        Bucket* next;

        Bucket() : id(-1), next(0) {}
        Bucket(const std::string& val) : val(val), id(-1), next(0) {}
        ~Bucket()
        {
            if (next) delete next;
        }

        template<typename T>
        Bucket* get(const T& val)
        {
            // std::string has efficient comparison for C-style strings
            if (this->val == val)
                return this;
            if (next)
                return next->get(val);
            return next = new Bucket(val);
        }
    };

    /// Hash table size
    size_t table_size;

    /// Hash table
    Bucket** table;

    /// Interned string table
    std::vector<std::string> string_table;


    StringInternTable(size_t table_size=1024);
    ~StringInternTable();

    // Get the intern ID for a string
    template<typename T>
    unsigned intern(const T& val)
    {
        unsigned pos = hash(val);
        Bucket* res = 0;
        if (table[pos])
            res = table[pos]->get(val);
        else
            res = table[pos] = new Bucket(val);
        if (res->id == -1)
        {
            res->id = string_table.size();
            // In case strings can share their memory representation, share the
            // reference to the hashtable buckets here
            string_table.push_back(res->val);
        }
        return res->id;
    }

    unsigned hash(const char *str);
    unsigned hash(const std::string& str);
};

}
}

#endif
