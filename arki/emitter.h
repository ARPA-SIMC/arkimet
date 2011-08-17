#ifndef ARKI_EMITTER_H
#define ARKI_EMITTER_H

/*
 * emitter - Arkimet structured data formatter
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

#include <string>
#include <stdint.h>

namespace arki {

/**
 * Abstract interface for all emitters
 */
class Emitter
{
public:
    // Public interface
    virtual ~Emitter() {}

    // Implementation
    virtual void start_list() = 0;
    virtual void end_list() = 0;

    virtual void start_mapping() = 0;
    virtual void end_mapping() = 0;

    virtual void add_null() = 0;
    virtual void add_bool(bool val) = 0;
    virtual void add_int(long long int val) = 0;
    virtual void add_double(double val) = 0;
    virtual void add_string(const std::string& val) = 0;

    // Shortcuts
    void add(const std::string& val) { add_string(val); }
    void add(const char* val) { add_string(val); }
    void add(double val) { add_double(val); }
    void add(int32_t val) { add_int(val); }
    void add(int64_t val) { add_int(val); }
    void add(uint32_t val) { add_int(val); }
    void add(uint64_t val) { add_int(val); }
    void add(bool val) { add_bool(val); }

    // Shortcut to add a mapping, which also ensure the key is a string
    template<typename T>
    void add(const std::string& a, T b)
    {
        add(a);
        add(b);
    }
};


}

// vim:set ts=4 sw=4:
#endif
