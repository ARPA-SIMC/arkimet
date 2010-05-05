/*
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

#include <arki/tests/test-utils.h>
#include <arki/metadata/collection.h>
#include <arki/utils/datareader.h>
#include <arki/scan/any.h>
#include <wibble/sys/buffer.h>
#include <wibble/sys/fs.h>

#include <cstdlib>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_datareader_shar {
};
TESTGRP(arki_utils_datareader);

// Read an uncompressed file
template<> template<>
void to::test<1>()
{
	utils::DataReader dr;
	
	wibble::sys::Buffer buf;
	buf.resize(7218);
	dr.read("inbound/test.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("inbound/test.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Read an uncompressed file without index
template<> template<>
void to::test<2>()
{
	utils::DataReader dr;

	sys::fs::deleteIfExists("testcompr.grib1");
	sys::fs::deleteIfExists("testcompr.grib1.gz");
	ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);
	ensure(system("gzip testcompr.grib1") == 0);
	
	wibble::sys::Buffer buf;
	buf.resize(7218);
	dr.read("testcompr.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("testcompr.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Read an uncompressed file with the index
template<> template<>
void to::test<3>()
{
	utils::DataReader dr;

	sys::fs::deleteIfExists("testcompr.grib1");
	sys::fs::deleteIfExists("testcompr.grib1.gz");
	ensure(system("cp inbound/test.grib1 testcompr.grib1") == 0);

	metadata::Collection mdc;
	scan::scan("testcompr.grib1", mdc);
	mdc.compressDataFile(2, "testcompr.grib1");
	sys::fs::deleteIfExists("testcompr.grib1");
	
	wibble::sys::Buffer buf;
	buf.resize(7218);
	dr.read("testcompr.grib1", 0, 7218, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 7214, 4), "7777");

	buf.resize(34960);
	dr.read("testcompr.grib1", 7218, 34960, buf.data());
	ensure_equals(string((const char*)buf.data(), 4), "GRIB");
	ensure_equals(string((const char*)buf.data() + 34956, 4), "7777");
}

// Don't segfault on nonexisting files
template<> template<>
void to::test<4>()
{
	utils::DataReader dr;

	sys::fs::deleteIfExists("test.grib1");

	wibble::sys::Buffer buf;
	buf.resize(7218);

	try {
		dr.read("test.grib1", 0, 7218, buf.data());
		ensure(false);
	} catch (std::exception& e) {
		ensure(string(e.what()).find("file does not exist") != string::npos);
	}
}


}

// vim:set ts=4 sw=4:
