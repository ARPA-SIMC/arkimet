#ifndef ARKI_TARGETFILE_H
#define ARKI_TARGETFILE_H

/*
 * arki/targetfile - Compute file names out of metadata
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

#include <arki/metadata.h>
#include <arki/utils/lua.h>
#include <string>

namespace arki {

class Targetfile
{
protected:
	Lua *L;

public:
	struct Func
	{
		Lua* L;
		int idx;

		Func(Lua* L, int idx) : L(L), idx(idx) {}

		/**
		 * Compute a target file name for a metadata
		 */
		std::string operator()(const Metadata& md);
	};
	
	Targetfile(const std::string& code = std::string());
	virtual ~Targetfile();

	/**
	 * Load definition from the rc files.
	 *
	 * This is called automatically by the costructor if the constructor
	 * code parameter is empty.
	 */
	void loadRCFiles();

	/**
	 * Get a target file generator
	 *
	 * @param def
	 *   Target file definition in the form "type:parms".
	 */
	Func get(const std::string& def);
};

}

// vim:set ts=4 sw=4:
#endif
