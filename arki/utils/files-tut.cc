/*
 * Copyright (C) 2007--2013  Enrico Zini <enrico@enricozini.org>
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

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::utils::files;

struct arki_utils_files_shar {
	arki_utils_files_shar()
	{
		system("rm -rf commontest");
		system("mkdir commontest");
	}
};
TESTGRP(arki_utils_files);

// Test rebuild flagfile creation
template<> template<>
void to::test<1>()
{
	string name = "commontest/a";
	ensure(!hasRebuildFlagfile(name));
	createRebuildFlagfile(name);
	ensure(hasRebuildFlagfile(name));
	try {
		createNewRebuildFlagfile(name);
		ensure(false);
	} catch (...) {
	}
	removeRebuildFlagfile(name);
	ensure(!hasRebuildFlagfile(name));
	createNewRebuildFlagfile(name);
	ensure(hasRebuildFlagfile(name));
	removeRebuildFlagfile(name);
	ensure(!hasRebuildFlagfile(name));
}

// Test pack flagfile creation
template<> template<>
void to::test<2>()
{
	string name = "commontest/a";
	ensure(!hasPackFlagfile(name));
	createPackFlagfile(name);
	ensure(hasPackFlagfile(name));
	try {
		createNewPackFlagfile(name);
		ensure(false);
	} catch (...) {
	}
	removePackFlagfile(name);
	ensure(!hasPackFlagfile(name));
	createNewPackFlagfile(name);
	ensure(hasPackFlagfile(name));
	removePackFlagfile(name);
	ensure(!hasPackFlagfile(name));
}

// Test index flagfile creation
template<> template<>
void to::test<3>()
{
	string name = "commontest";
	ensure(!hasIndexFlagfile(name));
	createIndexFlagfile(name);
	ensure(hasIndexFlagfile(name));
	try {
		createNewIndexFlagfile(name);
		ensure(false);
	} catch (...) {
	}
	removeIndexFlagfile(name);
	ensure(!hasIndexFlagfile(name));
	createNewIndexFlagfile(name);
	ensure(hasIndexFlagfile(name));
	removeIndexFlagfile(name);
	ensure(!hasIndexFlagfile(name));
}

// Test dontpack flagfile creation
template<> template<>
void to::test<4>()
{
	string name = "commontest";
	ensure(!hasDontpackFlagfile(name));
	createDontpackFlagfile(name);
	ensure(hasDontpackFlagfile(name));
	try {
		createNewDontpackFlagfile(name);
		ensure(false);
	} catch (...) {
	}
	removeDontpackFlagfile(name);
	ensure(!hasDontpackFlagfile(name));
	createNewDontpackFlagfile(name);
	ensure(hasDontpackFlagfile(name));
	removeDontpackFlagfile(name);
	ensure(!hasDontpackFlagfile(name));
}

// Test timestamp
template<> template<>
void to::test<5>()
{
	ensure_equals(timestamp("commontest/a"), 0);
	utils::createFlagfile("commontest/a");
	ensure(timestamp("commontest/a") != 0);
    sys::fs::unlink("commontest/a");
	ensure_equals(timestamp("commontest/a"), 0);
}

// Test readFile and writeFile
template<> template<>
void to::test<6>()
{
    using namespace arki::utils::files;
    string test("ciao");

    write_file("testfile", test);
    string test1 = read_file("testfile");
    atest(equals, test, test1);
}

// Test resolve_path
template<> template<>
void to::test<7>()
{
    using namespace arki::utils::files;
    string basedir, relname;

    resolve_path(".", basedir, relname);
    atest(equals, sys::fs::abspath("."), basedir);
    atest(equals, ".", relname);

    resolve_path("/tmp/foo", basedir, relname);
    atest(equals, "", basedir);
    atest(equals, "/tmp/foo", relname);

    resolve_path("foo/bar/../baz", basedir, relname);
    atest(equals, sys::fs::abspath("."), basedir);
    atest(equals, "foo/baz", relname);
}

// Test format_from_ext
template<> template<>
void to::test<8>()
{
    using namespace arki::utils::files;

    atest(equals, "grib", format_from_ext("test.grib"));
    atest(equals, "grib", format_from_ext("test.grib1"));
    atest(equals, "grib", format_from_ext("test.grib2"));
    atest(equals, "bufr", format_from_ext("test.bufr"));
#ifdef HAVE_ODIMH5
    atest(equals, "odimh5", format_from_ext("test.h5"));
    atest(equals, "odimh5", format_from_ext("test.hdf5"));
    atest(equals, "odimh5", format_from_ext("test.odim"));
    atest(equals, "odimh5", format_from_ext("test.odimh5"));
#endif
}

}

// vim:set ts=4 sw=4:
