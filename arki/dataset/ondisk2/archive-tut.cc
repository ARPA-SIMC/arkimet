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
#include <arki/dataset/ondisk2/writer.h>
#include <arki/configfile.h>
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

	ConfigFile cfg;

	arki_dataset_ondisk2_archive_shar()
	{
		system("rm -rf testds");
		system("mkdir testds");
		system("mkdir testds/.archive");
		system("mkdir testds/.archive/last");

		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "ondisk2");
		cfg.setValue("step", "daily");
		cfg.setValue("unique", "origin, reftime");
	}

	virtual void operator()(const std::string& file, State state) {}
};
TESTGRP(arki_dataset_ondisk2_archive);

// Acquire and query
template<> template<>
void to::test<1>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();

	// Acquire
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/index.sqlite", F_OK));

	metadata::Collector mdc;
	Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
	ensure_equals(mdc.size(), 3u);
	ensure_equals(mdc[0].source.upcast<source::Blob>()->filename, "test.grib1");

	// Query
	mdc.clear();
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

// Test maintenance scan on non-indexed files
template<> template<>
void to::test<2>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");

	// Query now is ok
	metadata::Collector mdc;
	arc.queryMetadata(Matcher(), false, mdc);
	ensure_equals(mdc.size(), 0u);

	// Maintenance should show one file to index
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_INDEX), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);
		MetadataCounter counter;

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, counter);
		ensure_equals(counter.count, 0u);
		ensure_equals(s.str(), 
				"testds: rescanned in archive last/test.grib1\n"
				"testds: archive cleaned up\n"
				"testds: database cleaned up\n"
				"testds: rebuild summary cache\n"
				"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<3>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/index.sqlite", F_OK));

	// Query now is ok
	metadata::Collector mdc;
	arc.queryMetadata(Matcher(), false, mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show one file to rescan
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_RESCAN), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);
		MetadataCounter counter;

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, counter);
		ensure_equals(counter.count, 0u);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Test maintenance scan on missing summary
template<> template<>
void to::test<4>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/index.sqlite", F_OK));

	// Query now is ok
	metadata::Collector mdc;
	arc.queryMetadata(Matcher(), false, mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show one file to rescan
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_RESCAN), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);
		MetadataCounter counter;

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, counter);
		ensure_equals(counter.count, 0u);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

}
