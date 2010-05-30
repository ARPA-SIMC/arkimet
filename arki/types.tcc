#ifndef ARKI_TYPES_TCC
#define ARKI_TYPES_TCC

/*
 * types - arkimet metadata type system
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

#include <wibble/exception.h>
#include <wibble/string.h>

#include "config.h"
#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

namespace arki {
namespace types {

template<typename BASE>
void CoreType<BASE>::lua_loadlib(lua_State* L)
{
	/* By default, do not register anything */
}

template<typename BASE>
void StyledType<BASE>::encodeWithoutEnvelope(utils::codec::Encoder& enc) const
{
	enc.addUInt(this->style(), 1);
}

template<typename BASE>
int StyledType<BASE>::compare(const Type& o) const
{
	using namespace wibble;

	int res = CoreType<BASE>::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const BASE* v = dynamic_cast<const BASE*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			str::fmtf("second element claims to be `%s', but is `%s' instead",
				traits<BASE>::type_tag, typeid(&o).name()));

	res = this->style() - v->style();
	if (res != 0) return res;
	return this->compare_local(*v);
}

#ifdef HAVE_LUA
template<typename TYPE>
bool StyledType<TYPE>::lua_lookup(lua_State* L, const std::string& name) const
{
        if (name == "style")
        {
		std::string s = TYPE::formatStyle(style());
                lua_pushlstring(L, s.data(), s.size());
                return true;
        }
	return false;
}
#endif


}
}

// vim:set ts=4 sw=4:
#endif
