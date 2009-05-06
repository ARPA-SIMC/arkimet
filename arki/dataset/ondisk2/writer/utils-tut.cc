/*
 * Copyright (C) 2007,2008,2009  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/test-utils.h>
#include <arki/dataset/ondisk2/writer/utils.h>
#include <arki/utils.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <fstream>

#include <sys/stat.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace wibble::sys;

struct arki_dataset_ondisk2_writer_utils_shar {
	arki_dataset_ondisk2_writer_utils_shar()
	{
	}
};
TESTGRP(arki_dataset_ondisk2_writer_utils);

// Test DirScanner on an empty directory
template<> template<>
void to::test<1>()
{
	system("rm -rf dirscanner");
	mkdir("dirscanner", 0777);

	DirScanner ds("dirscanner");

	ensure_equals(ds.cur(), string());
	ds.next();
	ensure_equals(ds.cur(), string());
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

	DirScanner ds("dirscanner");

	ensure_equals(ds.cur(), "2008/a.grib");
	ds.next();
	ensure_equals(ds.cur(), "2008/b.grib");
	ds.next();
	ensure_equals(ds.cur(), "2009/a.grib");
	ds.next();
	ensure_equals(ds.cur(), "2009/b.grib");
	ds.next();
	ensure_equals(ds.cur(), string());
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

	DirScanner ds("dirscanner");

	ensure_equals(ds.cur(), "2008/a.grib");
	ds.next();
	ensure_equals(ds.cur(), "2008/a/a.grib");
	ds.next();
	ensure_equals(ds.cur(), "2008/b.grib");
	ds.next();
	ensure_equals(ds.cur(), "2009/a.grib");
	ds.next();
	ensure_equals(ds.cur(), "2009/b.grib");
	ds.next();
	ensure_equals(ds.cur(), string());
}

}

// vim:set ts=4 sw=4:
