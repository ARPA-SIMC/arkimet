/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk2/test-utils.h>
#include <arki/dataset/ondisk2/archive.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/metadata.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace arki::types;
using namespace arki::utils;

struct arki_dataset_ondisk2_archive_shar : public MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	arki_dataset_ondisk2_archive_shar()
	{
		system("rm -rf testarc");
		system("mkdir testarc");
	}

	virtual void operator()(const std::string& file, State state) {}
};
TESTGRP(arki_dataset_ondisk2_archive);

// Acquire and query
template<> template<>
void to::test<1>()
{
	Archive arc("testarc");
	arc.openRW();

	// Acquire
	system("cp inbound/test.grib1 testarc/");
	arc.acquire("test.grib1");
	ensure(sys::fs::access("testarc/test.grib1", F_OK));
	ensure(sys::fs::access("testarc/test.grib1.metadata", F_OK));
	ensure(sys::fs::access("testarc/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testarc/index.sqlite", F_OK));

	// Query
	metadata::Collector mdc;
	arc.queryMetadata(Matcher(), false, mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show it's all ok
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	//c.dump(cerr);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Acquire obsolete data and pack
template<> template<>
void to::test<2>()
{
	Archive arc("testarc", 1);
	arc.openRW();
	system("cp inbound/test.grib1 testarc/");
	arc.acquire("test.grib1");
	ensure(sys::fs::access("testarc/test.grib1", F_OK));
	ensure(sys::fs::access("testarc/test.grib1.metadata", F_OK));
	ensure(sys::fs::access("testarc/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testarc/index.sqlite", F_OK));

	// Query now is ok
	metadata::Collector mdc;
	arc.queryMetadata(Matcher(), false, mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show it's all ok
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_DELETE), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());
}

}
