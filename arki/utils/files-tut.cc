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

#include <arki/tests/test-utils.h>
#include <arki/utils.h>
#include <arki/utils/files.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
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

// Test index flagfile creation
template<> template<>
void to::test<4>()
{
	ensure_equals(timestamp("commontest/a"), 0);
	utils::createFlagfile("commontest/a");
	ensure(timestamp("commontest/a") != 0);
	utils::removeFlagfile("commontest/a");
	ensure_equals(timestamp("commontest/a"), 0);
}

}

// vim:set ts=4 sw=4:
