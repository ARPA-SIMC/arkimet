/*
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

#include <arki/dataset/test-utils.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/maintenance.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/utils/metadata.h>
#include <arki/scan/any.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset::simple;

struct ForceSqlite
{
	bool old;

	ForceSqlite(bool val = true) : old(Manifest::get_force_sqlite())
	{
		Manifest::set_force_sqlite(val);
	}
	~ForceSqlite()
	{
		Manifest::set_force_sqlite(old);
	}
};

struct arki_dataset_simple_index_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	arki_dataset_simple_index_shar()
	{
		system("rm -rf testds");
		system("mkdir testds");
		system("mkdir testds/.archive");
		system("mkdir testds/.archive/last");
	}

	virtual void operator()(const std::string& file, State state) {}

	std::string idxfname() const
	{
		return Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
	}
};
TESTGRP(arki_dataset_simple_index);

// Acquire and query
template<> template<>
void to::test<1>()
{
	// Empty dirs do not show up as having a manifest
	ensure(!Manifest::exists("testds/.archive/last"));

	// An empty MANIFEST file counts as an empty manifest
	system("touch testds/.archive/last/MANIFEST");
	ensure(Manifest::exists("testds/.archive/last"));
	sys::fs::deleteIfExists("testds/.archive/last/MANIFEST");

	// Same if there is a sqlite manifest
	system("touch testds/.archive/last/index.sqlite");
	ensure(Manifest::exists("testds/.archive/last"));
	sys::fs::deleteIfExists("testds/.archive/last/index.sqlite");
}


// Test accessing empty manifests
template<> template<>
void to::test<2>()
{
	// Opening a missing manifest read only fails
	{
		ensure(!Manifest::exists("testds/.archive/last/" + idxfname()));
		std::auto_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		try {
			m->openRO();
			ensure(false);
		} catch (std::exception& e) {
			ensure(string(e.what()).find("does not exist") != string::npos);
		}
	}

	// But an empty dataset
	{
		std::auto_ptr<Manifest> m = Manifest::create("testds/.archive/last");
		m->openRW();
	}

	std::auto_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRO();

	vector<string> files;
	m->fileList(Matcher(), files);
	ensure(files.empty());
}

// Retest with sqlite
template<> template<> void to::test<3>() { ForceSqlite fs; test<2>(); }

// Test creating a new manifest
template<> template<>
void to::test<4>()
{
	// Opening a missing manifest read-write creates a new one
	ensure(!sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	std::auto_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRW();
	m->flush();
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	
	MaintenanceCollector c;
	m->check(c);
	ensure_equals(c.fileStates.size(), 0u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	m->vacuum();
}

// Retest with sqlite
template<> template<> void to::test<5>() { ForceSqlite fs; test<4>(); }


// Test adding and removing files
template<> template<>
void to::test<6>()
{
	system("cp inbound/test.grib1 testds/.archive/last/a.grib1");
	system("mkdir testds/.archive/last/foo");
	system("cp inbound/test.grib1 testds/.archive/last/foo/b.grib1");

	std::auto_ptr<Manifest> m = Manifest::create("testds/.archive/last");
	m->openRW();

	Summary s;
	metadata::Summarise summarise(s);
	scan::scan("inbound/test.grib1", summarise);

	m->acquire("a.grib1", 1000010, s);
	m->acquire("foo/b.grib1", 1000011, s);

	vector<string> files;
	m->fileList(Matcher(), files);
	ensure_equals(files.size(), 2u);
	ensure_equals(files[0], "a.grib1");
	ensure_equals(files[1], "foo/b.grib1");

	m->remove("a.grib1");
	files.clear();
	m->fileList(Matcher(), files);
	ensure_equals(files.size(), 1u);
	ensure_equals(files[0], "foo/b.grib1");
}

// Retest with sqlite
template<> template<> void to::test<7>() { ForceSqlite fs; test<6>(); }


}
