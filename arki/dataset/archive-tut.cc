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
#include <arki/dataset/archive.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/metadata/test-generator.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

namespace arki {
namespace tests {

#define ensure_dataset_clean(x, y, z) arki::tests::impl_ensure_dataset_clean(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
template<typename DS>
void impl_ensure_dataset_clean(const wibble::tests::Location& loc, DS& ds, size_t filecount, size_t resultcount)
{
	using namespace std;
	using namespace arki;
	using namespace arki::utils;

	MaintenanceCollector c;
	ds.maintenance(c);
	inner_ensure_equals(c.fileStates.size(), filecount);
	inner_ensure_equals(c.count(dataset::maintenance::MaintFileVisitor::ARC_OK), filecount);
	inner_ensure_equals(c.remaining(), string());
	inner_ensure(c.isClean());

	metadata::Collection mdc;
	ds.queryData(dataset::DataQuery(Matcher(), false), mdc);
	inner_ensure_equals(mdc.size(), resultcount);
}

}
}

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::dataset;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

struct arki_dataset_archive_shar : public DatasetTest {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State
	arki_dataset_archive_shar()
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
		cfg.setValue("archive age", "7");
	}

#define ensure_archive_clean(x, y, z) impl_ensure_archive_clean(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
	void impl_ensure_archive_clean(const wibble::tests::Location& loc, const std::string& dir, size_t filecount, size_t resultcount)
	{
		Archive arc(dir);
		arc.openRO();
		arki::tests::impl_ensure_dataset_clean(loc, arc, filecount, resultcount);
		inner_ensure(files::exists(str::joinpath(dir, arcidxfname())));
	}
};
TESTGRP(arki_dataset_archive);

// Acquire and query
template<> template<>
void to::test<1>()
{
	metadata::Collection mdc;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();

		// Acquire
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		arc.flush();
		ensure(files::exists("testds/.archive/last/test.grib1"));
		ensure(files::exists("testds/.archive/last/test.grib1.metadata"));
		ensure(files::exists("testds/.archive/last/test.grib1.summary"));
		ensure(files::exists("testds/.archive/last/" + arcidxfname()));

		Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
		ensure_equals(mdc.size(), 3u);
		ensure_equals(mdc[0].source.upcast<source::Blob>()->filename, "test.grib1");
	}

	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on non-indexed files
template<> template<>
void to::test<2>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collection mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 0u);

		// Maintenance should show one file to index
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(), 
				"testds: rescanned in archive last/test.grib1\n"
				"testds: archive cleaned up\n"
				"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<3>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		arc.flush();
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
		ensure(files::exists("testds/.archive/last/test.grib1"));
		ensure(!files::exists("testds/.archive/last/test.grib1.metadata"));
		ensure(!files::exists("testds/.archive/last/test.grib1.summary"));
		ensure(files::exists("testds/.archive/last/" + arcidxfname()));
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collection mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the database
	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing summary
template<> template<>
void to::test<4>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		arc.flush();
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
		ensure(files::exists("testds/.archive/last/test.grib1"));
		ensure(files::exists("testds/.archive/last/test.grib1.metadata"));
		ensure(!files::exists("testds/.archive/last/test.grib1.summary"));
		ensure(files::exists("testds/.archive/last/" + arcidxfname()));
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collection mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<5>()
{
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/1.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/2.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/3.grib1");
		arc.acquire("1.grib1");
		arc.acquire("2.grib1");
		arc.acquire("3.grib1");
		arc.flush();

		sys::fs::deleteIfExists("testds/.archive/last/2.grib1.metadata");
		ensure(files::exists("testds/.archive/last/2.grib1"));
		ensure(!files::exists("testds/.archive/last/2.grib1.metadata"));
		ensure(files::exists("testds/.archive/last/2.grib1.summary"));
		ensure(files::exists("testds/.archive/last/" + arcidxfname()));
	}

	// Query now is ok
	{
		metadata::Collection mdc;
		Archive arc("testds/.archive/last");
		arc.openRO();
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 9u);
	}

	// Maintenance should show one file to rescan
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		MaintenanceCollector c;
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		MaintenanceCollector c;
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/2.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 3, 9);
}

// Test maintenance scan on compressed archives
template<> template<>
void to::test<6>()
{
	MaintenanceCollector c;
	{
		// Import a file
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		arc.flush();

		// Compress it
		metadata::Collection mdc;
		Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
		ensure_equals(mdc.size(), 3u);
		mdc.compressDataFile(1024, "metadata file testds/.archive/last/test.grib1.metadata");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1");

		ensure(!files::exists("testds/.archive/last/test.grib1"));
		ensure(files::exists("testds/.archive/last/test.grib1.gz"));
		ensure(files::exists("testds/.archive/last/test.grib1.gz.idx"));
		ensure(files::exists("testds/.archive/last/test.grib1.metadata"));
		ensure(files::exists("testds/.archive/last/test.grib1.summary"));
		ensure(files::exists("testds/.archive/last/" + arcidxfname()));
	}

	// Everything is still ok
	ensure_archive_clean("testds/.archive/last", 1, 3);

	// Try removing summary and metadata
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");

	// Cannot query anymore
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collection mdc;
		try {
			arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
			ensure(false);
		} catch (std::exception& e) {
			ensure(str::startsWith(e.what(), "file needs to be manually decompressed before scanning."));
		}

		// Maintenance should show one file to rescan
		c.clear();
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test handling of empty archive dirs (such as last with everything moved away)
template<> template<>
void to::test<7>()
{
	// Import a file in a secondary archive
	{
		system("mkdir testds/.archive/foo");
		Archive arc("testds/.archive/foo");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/foo/");
		arc.acquire("test.grib1");
	}

	// Everything should be fine now
	Archives arc("testds/.archive");
	ensure_dataset_clean(arc, 1, 3);
}

template<> template<>
void to::test<8>()
{
    // Generate a dataset with archived data
    {
        // Override current date for maintenance to 2010-09-15
        dataset::ondisk2::TestOverrideCurrentDateForMaintenance od(1284505200);

        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));

        // Import several metadata items
        metadata::test::Generator gen("grib1");
        for (int i = 0; i < 30; ++i)
            gen.add(types::TYPE_REFTIME, str::fmtf("2010-09-%02dT07:00:00Z", i + 1));
        struct Importer : public metadata::Consumer
        {
            WritableLocal& ds;

            Importer(WritableLocal& ds) : ds(ds) {}
            bool operator()(Metadata& md)
            {
                WritableDataset::AcquireResult r = ds.acquire(md);
                ensure(r == WritableDataset::ACQ_OK);
                return true;
            }
        } importer(*ds);
        gen.generate(importer);

        // Check to remove new dataset marker
        stringstream checklog;
        ds->check(checklog, true, true);
        ensure_contains(checklog.str(), "testds: 30448 bytes reclaimed");

        // Archive old files
        stringstream packlog;
        ds->repack(packlog, true);
        ensure_contains(packlog.str(), "6 files archived");
    }

    auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));

    // Query summary
    Summary s1;
    ds->querySummary(Matcher::parse(""), s1);

    // Query data and summarise the results
    Summary s2;
    {
        metadata::Summarise sum(s2);
        ds->queryData(dataset::DataQuery(Matcher::parse("")), sum);
    }

    ensure(s1 == s2);
}

// Retest with sqlite
template<> template<> void to::test<9>() { ForceSqlite fs; test<1>(); }
template<> template<> void to::test<10>() { ForceSqlite fs; test<2>(); }
template<> template<> void to::test<11>() { ForceSqlite fs; test<3>(); }
template<> template<> void to::test<12>() { ForceSqlite fs; test<4>(); }
template<> template<> void to::test<13>() { ForceSqlite fs; test<5>(); }
template<> template<> void to::test<14>() { ForceSqlite fs; test<6>(); }
template<> template<> void to::test<15>() { ForceSqlite fs; test<7>(); }
template<> template<> void to::test<16>() { ForceSqlite fs; test<8>(); }


}
