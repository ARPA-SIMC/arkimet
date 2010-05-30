#ifndef ARKI_TYPES_ASSIGNEDDATASET_H
#define ARKI_TYPES_ASSIGNEDDATASET_H

/*
 * types/assigneddataset - Dataset assignment
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types.h>
#include <arki/types/time.h>

struct lua_State;

namespace arki {
namespace types {


struct AssignedDataset;

template<>
struct traits<AssignedDataset>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * A metadata annotation
 */
struct AssignedDataset : public types::CoreType<AssignedDataset>
{
	Item<types::Time> changed;
	std::string name;
	std::string id;

	AssignedDataset(const Item<types::Time>& changed, const std::string& name, const std::string& id)
		: changed(changed), name(name), id(id) {}

	virtual int compare(const AssignedDataset& o) const;
	virtual bool operator==(const Type& o) const;

	/// CODEC functions
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<AssignedDataset> decode(const unsigned char* buf, size_t len);
	static Item<AssignedDataset> decodeString(const std::string& val);
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	// Lua functions
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	/// Create a attributed dataset definition with the current time
	static Item<AssignedDataset> create(const std::string& name, const std::string& id);

	/// Create a attributed dataset definition with the givem time
	static Item<AssignedDataset> create(const Item<types::Time>& time, const std::string& name, const std::string& id);
};

}
}

// vim:set ts=4 sw=4:
#endif
