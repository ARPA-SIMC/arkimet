
#ifndef ARKI_SCAN_ODIMH5_H
#define ARKI_SCAN_ODIMH5_H

/*
 * scan/odimh5 - Scan a ODIMH5 file for metadata
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <string>
#include <vector>

#include <radarlib/odimh5v20.hpp>

/*============================================================================*/

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace odimh5 {
const Validator& validator();
}

/*============================================================================*/
/* ODIM H5 SCANNER */
/*============================================================================*/

class OdimH5
{
public:
	OdimH5();
	virtual ~OdimH5();

	/**
	 * Access a file with ODIMH5 data
	 */
	void open(const std::string& filename);
  /**
   * Access a file with ODIMH5 data  - alternate version with explicit
   * basedir/relname separation
   */
  void open(const std::string& filename, const std::string& basedir, const std::string& relname);

	/**
	 * Close the input file.
	 *
	 * This is optional: the file will be closed by the destructor if needed.
	 */
	void close();

	/**
	 * Scan the next ODIMH5 in the file.
	 *
	 * @returns
	 *   true if it found a GRIB message,
	 *   false if there are no more GRIB messages in the file
	 */
	bool next(Metadata& md);

protected:
	std::string filename;
	std::string basedir;
    std::string relname;
	OdimH5v20::OdimObject* odimObj; /* ODIMH5 loaded from input file */
	int 			read;

	/**
	 * Extract metadata from a OdimH5 object 
	 */
	virtual void getOdimObjectData(OdimH5v20::OdimObject* obj, Metadata& md);
	/**
	 * Extract metadata from a OdimH5 PVOL/SCAN object
	 * This method is called only for PVOL/SCAN objects
	 */
	virtual void getPVOLData(OdimH5v20::PolarVolume* obj, Metadata& md);
	/**
	 * Set the source information in the metadata
	 */
	virtual void setSource(Metadata& md);
};

/*============================================================================*/

}
}

// vim:set ts=4 sw=4:
#endif
