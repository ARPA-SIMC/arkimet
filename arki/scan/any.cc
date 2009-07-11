/*
 * scan/any - Scan files autodetecting the format
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/scan/any.h>
#include <arki/metadata.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace scan {

static void scan_metadata(const std::string& file, MetadataConsumer& c)
{
	//cerr << "Reading cached metadata from " << file << endl;
	Metadata::readFile(file, c);
}

static bool scan_file(const std::string& file, MetadataConsumer& c)
{
	// Get the file extension
	size_t pos = file.rfind('.');
	if (pos == string::npos)
		// No extension, we do not know what it is
		return false;
	string ext = file.substr(pos+1);

	// Scan the file
#ifdef HAVE_GRIBAPI
	if (ext == "grib1" || ext == "grib2")
	{
		scan::Grib scanner;
		scanner.open(file);
		Metadata md;
		while (scanner.next(md))
			c(md);
		return true;
	}
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr") {
		scan::Bufr scanner;
		scanner.open(file);
		Metadata md;
		while (scanner.next(md))
			c(md);
		return true;
	}
#endif
	return false;
}

bool scan(const std::string& file, MetadataConsumer& c)
{
	string md_fname = file + ".metadata";
	auto_ptr<struct stat> st_file = sys::fs::stat(file);
	if (!st_file.get())
		throw wibble::exception::File(file, "getting file information");
	auto_ptr<struct stat> st_md = sys::fs::stat(md_fname);

	if (st_md.get() and st_md->st_mtime >= st_file->st_mtime)
	{
		// If there is a metadata file, use it to save time
		scan_metadata(md_fname, c);
		return true;
	} else {
		return scan_file(file, c);
	}
}

bool canScan(const std::string& file)
{
	// Get the file extension
	size_t pos = file.rfind('.');
	if (pos == string::npos)
		// No extension, we do not know what it is
		return false;
	string ext = str::tolower(file.substr(pos+1));

	// Check for known extensions
#ifdef HAVE_GRIBAPI
	if (ext == "grib" || ext == "grib1" || ext == "grib2")
		return true;
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr")
		return true;
#endif
	return false;
}

}
}
// vim:set ts=4 sw=4:
