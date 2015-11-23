#ifndef ARKI_TYPES_SOURCE_INLINE_H
#define ARKI_TYPES_SOURCE_INLINE_H

/*
 * types/source - Source information
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

struct Inline : public Source
{
    uint64_t size;

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    const char* lua_type_name() const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;

    Inline* clone() const override;

    static std::unique_ptr<Inline> create(const std::string& format, uint64_t size);
    static std::unique_ptr<Inline> decodeMapping(const emitter::memory::Mapping& val);
};


}
}
}
#endif
