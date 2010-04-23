/*
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/scan/dir.h>
#include <arki/utils.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>

#include <algorithm>
#include <sstream>
#include <iostream>
#include <fstream>

#include <sys/stat.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble::sys;

struct arki_scan_dir_shar {
	arki_scan_dir_shar()
	{
	}
};
TESTGRP(arki_scan_dir);

// Test DirScanner on an empty directory
template<> template<>
void to::test<1>()
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);

	vector<string> ds = scan::dir("dirscanner");
	ensure(ds.empty());
}

// Test DirScanner on a populated directory
template<> template<>
void to::test<2>()
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);
	utils::createFlagfile("dirscanner/index.sqlite");
	mkdir("dirscanner/2007", 0777);
	mkdir("dirscanner/2008", 0777);
	utils::createFlagfile("dirscanner/2008/a.grib");
	utils::createFlagfile("dirscanner/2008/b.grib");
	mkdir("dirscanner/2008/temp", 0777);
	mkdir("dirscanner/2009", 0777);
	utils::createFlagfile("dirscanner/2009/a.grib");
	utils::createFlagfile("dirscanner/2009/b.grib");
	mkdir("dirscanner/2009/temp", 0777);
	mkdir("dirscanner/.archive", 0777);
	utils::createFlagfile("dirscanner/.archive/z.grib");

	vector<string> ds = scan::dir("dirscanner");
	sort(ds.begin(), ds.end());

	ensure_equals(ds.size(), 4u);
	ensure_equals(ds[0], "2008/a.grib");
	ensure_equals(ds[1], "2008/b.grib");
	ensure_equals(ds[2], "2009/a.grib");
	ensure_equals(ds[3], "2009/b.grib");
}

// Test file names interspersed with directory names
template<> template<>
void to::test<3>()
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);
	utils::createFlagfile("dirscanner/index.sqlite");
	mkdir("dirscanner/2008", 0777);
	utils::createFlagfile("dirscanner/2008/a.grib");
	utils::createFlagfile("dirscanner/2008/b.grib");
	mkdir("dirscanner/2008/a", 0777);
	utils::createFlagfile("dirscanner/2008/a/a.grib");
	mkdir("dirscanner/2009", 0777);
	utils::createFlagfile("dirscanner/2009/a.grib");
	utils::createFlagfile("dirscanner/2009/b.grib");

	vector<string> ds = scan::dir("dirscanner");
	sort(ds.begin(), ds.end());

	ensure_equals(ds.size(), 5u);
	ensure_equals(ds[0], "2008/a.grib");
	ensure_equals(ds[1], "2008/a/a.grib");
	ensure_equals(ds[2], "2008/b.grib");
	ensure_equals(ds[3], "2009/a.grib");
	ensure_equals(ds[4], "2009/b.grib");
}

}

// vim:set ts=4 sw=4:
