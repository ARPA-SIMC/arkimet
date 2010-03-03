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

/**
 * A metadata annotation
 */
struct AssignedDataset : public types::Type
{
	Item<types::Time> changed;
	std::string name;
	std::string id;

	AssignedDataset(const Item<types::Time>& changed, const std::string& name, const std::string& id)
		: changed(changed), name(name), id(id) {}

	virtual int compare(const Type& o) const;
	virtual int compare(const AssignedDataset& o) const;
	virtual bool operator==(const Type& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<AssignedDataset> decode(const unsigned char* buf, size_t len);
	static Item<AssignedDataset> decodeString(const std::string& val);
	static types::Code typecode();
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	// LUA functions
	/// Push to the LUA stack a userdata to access this AssignedDataset
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the AssignedDataset LUA object
	static int lua_lookup(lua_State* L);

	/// Create a attributed dataset definition with the current time
	static Item<AssignedDataset> create(const std::string& name, const std::string& id);

	/// Create a attributed dataset definition with the givem time
	static Item<AssignedDataset> create(const Item<types::Time>& time, const std::string& name, const std::string& id);
};

}
}

// vim:set ts=4 sw=4:
#endif
