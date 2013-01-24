#ifndef ARKI_SCAN_GRIB_H
#define ARKI_SCAN_GRIB_H

/*
 * scan/grib - Scan a GRIB (version 1 or 2) file for metadata
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <vector>

struct grib_context;
struct grib_handle;
struct lua_State;

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace grib {
const Validator& validator();
}

class Grib;
struct GribLua;

class Grib
{
protected:
	std::string filename;
	std::string basedir;
	std::string relname;
	FILE* in;
	grib_context* context;
	grib_handle* gh;
	GribLua* L;
	bool m_inline_data;
	std::vector<int> grib1_funcs;
	std::vector<int> grib2_funcs;

	/**
	 * Set the source information in the metadata
	 */
	virtual void setSource(Metadata& md);

	/**
	 * Run Lua scanning functions on \a md
	 */
	bool scanLua(std::vector<int> ids, Metadata& md);

	/**
	 * Read GRIB1 data from the currently open handle into md
	 */
	void scanGrib1(Metadata& md);

	/**
	 * Read GRIB2 data from the currently open handle into md
	 */
	void scanGrib2(Metadata& md);

	static int arkilua_lookup_grib(lua_State* L);
	static int arkilua_lookup_gribl(lua_State* L);
	static int arkilua_lookup_gribs(lua_State* L);
	static int arkilua_lookup_gribd(lua_State* L);

public:
	Grib(bool inlineData = false,
		const std::string& grib1code = std::string(),
		const std::string& grib2code = std::string());
	virtual ~Grib();

	/**
	 * Access a file with GRIB data
	 */
	void open(const std::string& filename);

    /// Alternate version with explicit basedir/relname separation
    void open(const std::string& filename, const std::string& basedir, const std::string& relname);

	/**
	 * Close the input file.
	 *
	 * This is optional: the file will be closed by the destructor if needed.
	 */
	void close();

	/**
	 * Scan the next GRIB in the file.
	 *
	 * @returns
	 *   true if it found a GRIB message,
	 *   false if there are no more GRIB messages in the file
	 */
	bool next(Metadata& md);

	friend class GribLua;
};


class MultiGrib : public Grib
{
protected:
    std::string tmpfilename;
    std::ostream& tmpfile;

	/**
	 * Set the source information in the metadata.
	 *
	 * In the case of multigribs, we need to first copy the data to a temporary
	 * file, then use that as the source.  This rebuilds a scattered multigrib
	 * into a single blob of data.
	 */
	virtual void setSource(Metadata& md);

public:
	MultiGrib(const std::string& tmpfilename, std::ostream& tmpfile);
};

}
}

// vim:set ts=4 sw=4:
#endif
