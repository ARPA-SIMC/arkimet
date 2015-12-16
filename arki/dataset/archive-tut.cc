#include "arki/dataset/tests.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/ondisk2/writer.h"
#include "arki/dataset/test-scenario.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/files.h"
#include "arki/iotrace.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

template<typename DS>
static void ensure_dataset_clean(DS& ds, size_t filecount, size_t resultcount)
{
    MaintenanceCollector c;
    ds.maintenance(c);
    wassert(actual(c.fileStates.size()) == filecount);
    wassert(actual(c.count(DatasetTest::COUNTED_ARC_OK)) == filecount);
    wassert(actual(c.remaining()) == string());
    wassert(actual(c.isClean()));

    metadata::Collection mdc(ds, dataset::DataQuery(Matcher()));
    wassert(actual(mdc.size()) == resultcount);
}


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

        iotrace::init();
	}

#define ensure_archive_clean(x, y, z) wassert(impl_ensure_archive_clean((x), (y), (z)))
    void impl_ensure_archive_clean(const std::string& dir, size_t filecount, size_t resultcount)
    {
        unique_ptr<Archive> arc(Archive::create(dir));
        wassert(ensure_dataset_clean(*arc, filecount, resultcount));
        wassert(actual_file(str::joinpath(dir, arcidxfname())).exists());
    }
};
TESTGRP(arki_dataset_archive);

// Acquire and query
template<> template<>
void to::test<1>()
{
	metadata::Collection mdc;
	{
		unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));

        // Acquire
        system("cp inbound/test.grib1 testds/.archive/last/");
        arc->acquire("test.grib1");
        arc->flush();
        ensure(sys::exists("testds/.archive/last/test.grib1"));
        ensure(sys::exists("testds/.archive/last/test.grib1.metadata"));
        ensure(sys::exists("testds/.archive/last/test.grib1.summary"));
        ensure(sys::exists("testds/.archive/last/" + arcidxfname()));

        mdc.read_from_file("testds/.archive/last/test.grib1.metadata");
        ensure_equals(mdc.size(), 3u);
        ensure_equals(mdc[0].sourceBlob().filename, "test.grib1");
    }

	ensure_archive_clean("testds/.archive/last", 1, 3);

    // Check that querying the dataset gives results sorted by reftime
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last", false));
        OnlineArchive* a = dynamic_cast<OnlineArchive*>(arc.get());
        ensure(a);
        OrderCheck oc("reftime");
        a->query_data(dataset::DataQuery(), [&](unique_ptr<Metadata> md) { return oc.eat(move(md)); });
    }
}

// Test maintenance scan on non-indexed files
template<> template<>
void to::test<2>()
{
	MaintenanceCollector c;
	{
		unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
		system("cp inbound/test.grib1 testds/.archive/last/");
	}

    // Query now is ok
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));
        metadata::Collection mdc(*arc, dataset::DataQuery(Matcher()));
        ensure_equals(mdc.size(), 0u);

		// Maintenance should show one file to index
		arc->maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(), 
				"testds: rescanned in archive last/test.grib1\n"
				"testds: archive cleaned up\n"
				"testds: 1 file rescanned.\n");

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
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
        system("cp inbound/test.grib1 testds/.archive/last/");
        arc->acquire("test.grib1");
        arc->flush();
        sys::unlink_ifexists("testds/.archive/last/test.grib1.metadata");
        sys::unlink_ifexists("testds/.archive/last/test.grib1.summary");
        ensure(sys::exists("testds/.archive/last/test.grib1"));
        ensure(!sys::exists("testds/.archive/last/test.grib1.metadata"));
        ensure(!sys::exists("testds/.archive/last/test.grib1.summary"));
        ensure(sys::exists("testds/.archive/last/" + arcidxfname()));
    }

    // Query now is ok
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));
        metadata::Collection mdc(*arc, dataset::DataQuery(Matcher()));
        ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc->maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the database
	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned.\n");

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
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
        system("cp inbound/test.grib1 testds/.archive/last/");
        arc->acquire("test.grib1");
        arc->flush();
        sys::unlink_ifexists("testds/.archive/last/test.grib1.summary");
        ensure(sys::exists("testds/.archive/last/test.grib1"));
        ensure(sys::exists("testds/.archive/last/test.grib1.metadata"));
        ensure(!sys::exists("testds/.archive/last/test.grib1.summary"));
        ensure(sys::exists("testds/.archive/last/" + arcidxfname()));
    }

	// Query now is ok
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));
        metadata::Collection mdc(*arc, dataset::DataQuery(Matcher()));
        ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc->maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned.\n");

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
		unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
		system("cp inbound/test.grib1 testds/.archive/last/1.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/2.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/3.grib1");
		arc->acquire("1.grib1");
		arc->acquire("2.grib1");
		arc->acquire("3.grib1");
		arc->flush();

        sys::unlink_ifexists("testds/.archive/last/2.grib1.metadata");
        ensure(sys::exists("testds/.archive/last/2.grib1"));
        ensure(!sys::exists("testds/.archive/last/2.grib1.metadata"));
        ensure(sys::exists("testds/.archive/last/2.grib1.summary"));
        ensure(sys::exists("testds/.archive/last/" + arcidxfname()));
    }

    // Query now is ok
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));
        metadata::Collection mdc(*arc, Matcher());
        ensure_equals(mdc.size(), 9u);
    }

	// Maintenance should show one file to rescan
	{
		unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
		MaintenanceCollector c;
		arc->maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		MaintenanceCollector c;
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/2.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_ARC_OK), 3u);
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
		unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc->acquire("test.grib1");
		arc->flush();

        // Compress it
        metadata::Collection mdc;
        mdc.read_from_file("testds/.archive/last/test.grib1.metadata");
        ensure_equals(mdc.size(), 3u);
        mdc.compressDataFile(1024, "metadata file testds/.archive/last/test.grib1.metadata");
        sys::unlink_ifexists("testds/.archive/last/test.grib1");

        ensure(!sys::exists("testds/.archive/last/test.grib1"));
        ensure(sys::exists("testds/.archive/last/test.grib1.gz"));
        ensure(sys::exists("testds/.archive/last/test.grib1.gz.idx"));
        ensure(sys::exists("testds/.archive/last/test.grib1.metadata"));
        ensure(sys::exists("testds/.archive/last/test.grib1.summary"));
        ensure(sys::exists("testds/.archive/last/" + arcidxfname()));
    }

    // Everything is still ok
    ensure_archive_clean("testds/.archive/last", 1, 3);

    // Try removing summary and metadata
    sys::unlink_ifexists("testds/.archive/last/test.grib1.metadata");
    sys::unlink_ifexists("testds/.archive/last/test.grib1.summary");

    // Cannot query anymore
    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));
        metadata::Collection mdc;
        try {
            mdc.add(*arc, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }

		// Maintenance should show one file to rescan
		c.clear();
		arc->maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		ondisk2::Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: 1 file rescanned.\n");

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
		unique_ptr<Archive> arc(Archive::create("testds/.archive/foo", true));
		system("cp inbound/test.grib1 testds/.archive/foo/");
		arc->acquire("test.grib1");
	}

    // Everything should be fine now
    Archives arc("testds", "testds/.archive");
    wassert(ensure_dataset_clean(arc, 1, 3));
}

template<> template<>
void to::test<8>()
{
    using namespace arki::dataset;

    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last", true));

        // Acquire
        system("cp inbound/test.grib1 testds/.archive/last/");
        arc->acquire("test.grib1");
        arc->flush();
    }
    ensure_archive_clean("testds/.archive/last", 1, 3);

    metadata::Collection mdc;

    {
        unique_ptr<Archive> arc(Archive::create("testds/.archive/last"));

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 0), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 1), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 2), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 3), 0u);;
        ensure_equals(mdc.size(), 0u);
    }

    {
        unique_ptr<Archives> arc(new Archives("testds", "testds/.archive"));

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 0), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 1), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 2), 1u);;
        ensure_equals(mdc.size(), 1u);

        mdc.clear();
        ensure_equals(arc->produce_nth(mdc.inserter_func(), 3), 0u);;
        ensure_equals(mdc.size(), 0u);
    }
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
template<> template<> void to::test<17>() { ForceSqlite fs; test<9>(); }

template<> template<>
void to::test<18>()
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-archived");

    unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(scen.cfg));

    // Query summary
    Summary s1;
    ds->querySummary(Matcher::parse(""), s1);

    // Query data and summarise the results
    Summary s2;
    ds->query_data(Matcher(), [&](unique_ptr<Metadata> md) { s2.add(*md); return true; });

    // Verify that the time range of the first summary is what we expect
    unique_ptr<reftime::Period> p = downcast<reftime::Period>(s1.getReferenceTime());
    ensure_equals(p->begin, Time(2010, 9, 1, 0, 0, 0));
    ensure_equals(p->end, Time(2010, 9, 30, 0, 0, 0));

    ensure(s1 == s2);
}

template<> template<>
void to::test<19>()
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-manyarchivestates");

    // If dir.summary exists, don't complain if dir is missing
    {
        unique_ptr<Archive> a(Archive::create(str::joinpath(scen.path, ".archive/offline")));
        MaintenanceCollector c;
        a->maintenance(c);
        ensure(c.isClean());
    }

    // Querying dir uses dir.summary if dir is missing
    {
        unique_ptr<Archive> a(Archive::create(str::joinpath(scen.path, ".archive/offline")));
        Summary s;
        a->querySummary(Matcher::parse(""), s);
        ensure(s.count() > 0);
    }

    // Query the summary of the whole dataset and ensure it also spans .archive/offline
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(scen.cfg));
        Summary s;
        iotrace::Collector ioc;
        ds->querySummary(Matcher::parse(""), s);

        unique_ptr<reftime::Period> p = downcast<reftime::Period>(s.getReferenceTime());
        ensure_equals(p->begin, Time(2010, 9, 1, 0, 0, 0));
        ensure_equals(p->end, Time(2010, 9, 18, 0, 0, 0));
    }

    // TODO If dir.summary and dir is not missing, check dir contents but don't repair them
    // TODO If dir.summary and dir is not missing, also check that dir/summary matches dir.summary

    // If dir.summary exists, don't complain if dir is missing or if there are
    // problems inside dir
    {
        unique_ptr<Writer> ds(Writer::create(scen.cfg));
        // TODO: maintenance
    }
}

// Check behaviour of global archive summary cache
template<> template<>
void to::test<20>()
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-manyarchivestates");

    // Check that archive cache works
    unique_ptr<ReadonlyDataset> d(ReadonlyDataset::create(scen.cfg));
    Summary s;

    // Access the datasets so we don't count manifest reads in the iostats below
    d->querySummary(Matcher(), s);

    // Query once without cache, it should scan all the archives
    sys::unlink_ifexists(str::joinpath(scen.path, ".summaries/archives.summary"));
    {
        iotrace::Collector ioc;
        d->querySummary(Matcher(), s);
        ensure_equals(ioc.events.size(), 5u);
    }

    // Rebuild the cache
    {
        unique_ptr<Archives> a(new Archives(scen.path, str::joinpath(scen.path, ".archive"), false));
        a->vacuum();
    }

    // Query again, now we have the cache and it should do much less I/O
    {
        iotrace::Collector ioc;
        d->querySummary(Matcher(), s);
        ensure_equals(ioc.events.size(), 2u);
    }

    // Query without cache and with a reftime bound, it should still scan the datasets
    {
        iotrace::Collector ioc;
        d->querySummary(Matcher::parse("reftime:>=2010-09-10"), s);
        ensure_equals(ioc.events.size(), 6u);
    }
}


}
