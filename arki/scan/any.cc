/*
 * scan/any - Scan files autodetecting the format
 *
 * Copyright (C) 2009--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifdef HAVE_ODIMH5
#include <arki/scan/odimh5.h>
#endif
#ifdef HAVE_VM2
#include <arki/scan/vm2.h>
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

static bool scan_file(const std::string& pathname, const std::string& basedir, const std::string& relname, const std::string& format, metadata::Consumer& c)
{
    // Scan the file
    if (isCompressed(pathname))
        throw wibble::exception::Consistency("scanning " + relname + ".gz", "file needs to be manually decompressed before scanning");

#ifdef HAVE_GRIBAPI
    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        scan::Grib scanner;
        scanner.open(pathname, basedir, relname);
        Metadata md;
        while (scanner.next(md))
            c(md);
        return true;
    }
#endif
#ifdef HAVE_DBALLE
    if (format == "bufr") {
        scan::Bufr scanner;
        scanner.open(pathname, basedir, relname);
        Metadata md;
        while (scanner.next(md))
            c(md);
        return true;
    }
#endif
#ifdef HAVE_ODIMH5
    if ((format == "h5") || (format == "odim") || (format == "odimh5")) {
        scan::OdimH5 scanner;
        scanner.open(pathname, basedir, relname);
        Metadata md;
        while (scanner.next(md))
            c(md);
        return true;
    }
#endif
#ifdef HAVE_VM2
    if (format == "vm2") {
        scan::Vm2 scanner;
        scanner.open(pathname, basedir, relname);
        Metadata md;
        while (scanner.next(md))
            c(md);
        return true;
    }
#endif
	return false;
}

static std::string guess_format(const std::string& basedir, const std::string& file)
{
    // Get the file extension
    size_t pos = file.rfind('.');
    if (pos == string::npos)
        // No extension, we do not know what it is
        return std::string();
    return str::tolower(file.substr(pos+1));
}

bool scan(const std::string& basedir, const std::string& relname, metadata::Consumer& c)
{
    std::string format = guess_format(basedir, relname);

    // If we cannot detect a format, fail
    if (format.empty()) return false;
    return scan(basedir, relname, c, format);
}

bool scan(const std::string& basedir, const std::string& relname, metadata::Consumer& c, const std::string& format)
{
    // If we scan standard input, assume uncompressed data and do not try to
    // look for an existing .metadata file
    if (relname == "-")
        return scan_file(relname, basedir, relname, format, c);

    // stat the file (or its compressed version)
    string pathname = str::joinpath(basedir, relname);
    auto_ptr<struct stat> st_file = sys::fs::stat(pathname);
    if (!st_file.get())
        st_file = sys::fs::stat(pathname + ".gz");
    if (!st_file.get())
        throw wibble::exception::File(pathname, "getting file information");

    // stat the metadata file, if it exists
    string md_pathname = pathname + ".metadata";
    auto_ptr<struct stat> st_md = sys::fs::stat(md_pathname);

    if (st_md.get() and st_md->st_mtime >= st_file->st_mtime)
    {
        // If there is a usable metadata file, use it to save time
        scan_metadata(md_pathname, c);
        return true;
    } else {
        return scan_file(pathname, basedir, relname, format, c);
    }
}

bool scan(const std::string& file, metadata::Consumer& c)
{
    string basedir;
    string relname;
    utils::files::resolve_path(file, basedir, relname);
    return scan(basedir, relname, c);
}

bool scan(const std::string& file, metadata::Consumer& c, const std::string& format)
{
    string basedir;
    string relname;
    utils::files::resolve_path(file, basedir, relname);
    return scan(basedir, relname, c, format);
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
#ifdef HAVE_ODIMH5
	if ((ext == "h5") || (ext == "odimh5") || (ext == "odim"))
		return true;
#endif
#ifdef HAVE_VM2
    if (ext == "vm2")
        return true;
#endif
	return false;
}

bool exists(const std::string& file)
{
	if (sys::fs::exists(file)) return true;
	if (sys::fs::exists(file + ".gz")) return true;
	return false;
}

bool isCompressed(const std::string& file)
{
    return !sys::fs::exists(file) && sys::fs::exists(file + ".gz");
}

time_t timestamp(const std::string& file)
{
    time_t res = sys::fs::timestamp(file, 0);
    if (res != 0) return res;
    return sys::fs::timestamp(file + ".gz", 0);
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
#ifdef HAVE_ODIMH5
	if ((ext == "h5") || (ext == "odimh5") || (ext == "odim"))
		return odimh5::validator();
#endif
#ifdef HAVE_VM2
   if (ext == "vm2")
       return vm2::validator();
#endif
	throw wibble::exception::Consistency("looking for a validator for " + filename, "no validator available");
}

bool update_sequence_number(const Metadata& md, int& usn)
{
#ifdef HAVE_DBALLE
    // Update Sequence Numbers are only supported by BUFR
    if (md.source->format != "bufr")
        return false;

    wibble::sys::Buffer data = md.getData();
    string buf((const char*)data.data(), data.size());
    usn = Bufr::update_sequence_number(buf);
    return true;
#else
    return false;
#endif
}

wibble::sys::Buffer reconstruct(const std::string& format, const Metadata& md, const std::string& value)
{
#ifdef HAVE_VM2
    if (format == "vm2")
    {
        return scan::Vm2::reconstruct(md, value);
    }
#endif
    throw wibble::exception::Consistency("reconstructing " + format + " data", "format not supported");
}

}
}
// vim:set ts=4 sw=4:
