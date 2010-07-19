/*
 * scan/any - Scan files autodetecting the format
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata/consumer.h>
#include <arki/utils/files.h>
#include <arki/utils/compress.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>
#include <utime.h>

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace scan {

static void scan_metadata(const std::string& file, metadata::Consumer& c)
{
	//cerr << "Reading cached metadata from " << file << endl;
	Metadata::readFile(file, c);
}

static bool scan_file(const std::string& file, const std::string& format, metadata::Consumer& c)
{
	// Scan the file
	if (!files::exists(file) && files::exists(file + ".gz"))
		throw wibble::exception::Consistency("scanning " + file + ".gz", "file needs to be manually decompressed before scanning");

#ifdef HAVE_GRIBAPI
	if (format == "grib" || format == "grib1" || format == "grib2")
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
	if (format == "bufr") {
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

static bool scan_file(const std::string& file, metadata::Consumer& c)
{
	// Get the file extension
	size_t pos = file.rfind('.');
	if (pos == string::npos)
		// No extension, we do not know what it is
		return false;
	return scan_file(file, str::tolower(file.substr(pos+1)), c);
}

bool scan(const std::string& file, metadata::Consumer& c)
{
	string md_fname = file + ".metadata";
	auto_ptr<struct stat> st_file = sys::fs::stat(file);
	if (!st_file.get())
		st_file = sys::fs::stat(file + ".gz");
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

bool scan(const std::string& file, const std::string& format, metadata::Consumer& c)
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
		return scan_file(file, format, c);
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

bool exists(const std::string& file)
{
	if (files::exists(file)) return true;
	if (files::exists(file + ".gz")) return true;
	return false;
}

bool isCompressed(const std::string& file)
{
        return !files::exists(file) && files::exists(file + ".gz");
}

time_t timestamp(const std::string& file)
{
	time_t res = files::timestamp(file);
	if (res != 0) return res;
	return files::timestamp(file + ".gz");
}

void compress(const std::string& file, size_t groupsize)
{
	utils::compress::DataCompressor compressor(file, groupsize);
	scan(file, compressor);
	compressor.flush();

	// Set the same timestamp as the uncompressed file
	std::auto_ptr<struct stat> st = sys::fs::stat(file);
	struct utimbuf times;
	times.actime = st->st_atime;
	times.modtime = st->st_mtime;
	utime((file + ".gz").c_str(), &times);
	utime((file + ".gz.idx").c_str(), &times);

	// TODO: delete uncompressed version
}

const Validator& Validator::by_filename(const std::string& filename)
{
	// Get the file extension
	size_t pos = filename.rfind('.');
	if (pos == string::npos)
		// No extension, we do not know what it is
		throw wibble::exception::Consistency("looking for a validator for " + filename, "file name has no extension");
	string ext = str::tolower(filename.substr(pos+1));

#ifdef HAVE_GRIBAPI
	if (ext == "grib" || ext == "grib1" || ext == "grib2")
		return grib::validator();
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr")
		return bufr::validator();
#endif
	throw wibble::exception::Consistency("looking for a validator for " + filename, "no validator available");
}

}
}
// vim:set ts=4 sw=4:
