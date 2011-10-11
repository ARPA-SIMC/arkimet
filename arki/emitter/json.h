#ifndef ARKI_EMITTER_JSON_H
#define ARKI_EMITTER_JSON_H

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

#include <arki/emitter.h>
#include <vector>
#include <iosfwd>

namespace arki {
namespace emitter {

/**
 * JSON emitter
 */
class JSON : public Emitter
{
protected:
    enum State {
        LIST_FIRST,
        LIST,
        MAPPING_KEY_FIRST,
        MAPPING_KEY,
        MAPPING_VAL,
    };
    std::ostream& out;
    std::vector<State> stack;

    void val_head();

public:
    JSON(std::ostream& out);
    virtual ~JSON();

    virtual void start_list();
    virtual void end_list();

    virtual void start_mapping();
    virtual void end_mapping();

    virtual void add_null();
    virtual void add_bool(bool val);
    virtual void add_int(long long int val);
    virtual void add_double(double val);
    virtual void add_string(const std::string& val);

    virtual void add_raw(const std::string& val);
    virtual void add_raw(const wibble::sys::Buffer& val);

    static void parse(std::istream& in, Emitter& e);
};

}
}

// vim:set ts=4 sw=4:
#endif
