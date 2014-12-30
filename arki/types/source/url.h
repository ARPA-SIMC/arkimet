#ifndef ARKI_TYPES_SOURCE_URL_H
#define ARKI_TYPES_SOURCE_URL_H

/*
 * types/source - Source information
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/source.h>

namespace arki {
namespace types {
namespace source {

struct URL : public Source
{
    std::string url;

    virtual Style style() const;
    virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
    virtual std::ostream& writeToOstream(std::ostream& o) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;
    virtual const char* lua_type_name() const;
    virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    virtual int compare_local(const Source& o) const;
    virtual bool operator==(const Type& o) const;

    virtual wibble::sys::Buffer loadData() const;

    virtual uint64_t getSize() const;

    static Item<URL> create(const std::string& format, const std::string& url);
    static Item<URL> decodeMapping(const emitter::memory::Mapping& val);
};


}
}
}
#endif
