#include "arki/dataset/tests.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/reader.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/grib.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;

namespace {

static inline const types::AssignedDataset* getDataset(const Metadata& md)
{
    return md.get<AssignedDataset>();
}

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=simple
            step=daily
        )");
    }

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1")
    {
        DatasetTest::clean_and_import(nullptr, testfile);
        wassert(ensure_localds_clean(3, 3));
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_simple_writer");

void Tests::register_tests() {

// Add here only simple-specific tests that are not convered by tests in dataset-writer-test.cc

// Test acquiring data
add_method("acquire", [](Fixture& f) {
    // Clean the dataset
    system("rm -rf testds");
    system("mkdir testds");

    metadata::Collection mdc("inbound/test.grib1");
    Metadata& md = mdc[0];

    simple::Writer writer(f.cfg);

    // Import once in the empty dataset
    Writer::AcquireResult res = writer.acquire(md);
    ensure_equals(res, Writer::ACQ_OK);
    #if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
    #endif
    const AssignedDataset* ds = getDataset(md);
    ensure_equals(ds->name, "testds");
    ensure_equals(ds->id, "2007/07-08.grib1:0");

    wassert(actual_type(md.source()).is_source_blob("grib1", sys::abspath("./testds"), "2007/07-08.grib1", 0, 7218));

    // Import again works fine
    res = writer.acquire(md);
    ensure_equals(res, Writer::ACQ_OK);
    ds = getDataset(md);
    ensure_equals(ds->name, "testds");
    ensure_equals(ds->id, "2007/07-08.grib1:7218");

    wassert(actual_type(md.source()).is_source_blob("grib1", sys::abspath("./testds"), "2007/07-08.grib1", 7218, 7218));

    // Flush the changes and check that everything is allright
    writer.flush();
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
    ensure(sys::timestamp("testds/2007/07-08.grib1") <= sys::timestamp("testds/2007/07-08.grib1.metadata"));
    ensure(sys::timestamp("testds/2007/07-08.grib1.metadata") <= sys::timestamp("testds/2007/07-08.grib1.summary"));
    ensure(sys::timestamp("testds/2007/07-08.grib1.summary") <= sys::timestamp("testds/" + f.idxfname()));
    ensure(files::hasDontpackFlagfile("testds"));

    wassert(f.ensure_localds_clean(1, 2));
});

// Test appending to existing files
add_method("append", [](Fixture& f) {
    f.cfg.setValue("step", "yearly");

    metadata::Collection mdc("inbound/test-sorted.grib1");

    // Import once in the empty dataset
    {
        auto writer = f.makeSimpleWriter();
        Writer::AcquireResult res = writer->acquire(mdc[0]);
        ensure_equals(res, Writer::ACQ_OK);
    }

    // Import another one, appending to the file
    {
        auto writer = f.makeSimpleWriter();
        Writer::AcquireResult res = writer->acquire(mdc[1]);
        ensure_equals(res, Writer::ACQ_OK);

        const AssignedDataset* ds = getDataset(mdc[1]);
        ensure(ds);
        ensure_equals(ds->name, "testds");
        ensure_equals(ds->id, "20/2007.grib1:34960");

        wassert(actual_type(mdc[1].source()).is_source_blob("grib1", sys::abspath("testds"), "20/2007.grib1", 34960, 7218));
    }

    ensure(sys::exists("testds/20/2007.grib1"));
    ensure(sys::exists("testds/20/2007.grib1.metadata"));
    ensure(sys::exists("testds/20/2007.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
    ensure(sys::timestamp("testds/20/2007.grib1") <= sys::timestamp("testds/20/2007.grib1.metadata"));
    ensure(sys::timestamp("testds/20/2007.grib1.metadata") <= sys::timestamp("testds/20/2007.grib1.summary"));
    ensure(sys::timestamp("testds/20/2007.grib1.summary") <= sys::timestamp("testds/" + f.idxfname()));

    // Dataset is fine and clean
    wassert(f.ensure_localds_clean(1, 2));
});

// Test maintenance scan on non-indexed files
add_method("scan_nonindexed", [](Fixture& f) {
    f.cfg.setValue("step", "monthly");

    // Create a new segment, not in the index
    testdata::GRIBData data;
    sys::makedirs("testds/2007");
    // TODO: use segments also in the other tests, and instantiate a new test suite for different segment types
    Segment* s = f.segments().get_segment("2007/07.grib1");
    s->append(data.test_data[1].md);
    s->append(data.test_data[0].md);

    // Query now is ok, but empty
    {
        unique_ptr<simple::Reader> reader(f.makeSimpleReader());
        metadata::Collection mdc(*reader, Matcher());
        ensure_equals(mdc.size(), 0u);
    }

    // Maintenance should show one file to index
    {
        dataset::simple::Checker writer(f.cfg);
        MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_NEW] = 1;
        wassert(actual(writer).maintenance(expected));
        ensure(files::hasDontpackFlagfile("testds"));
    }

    {
        dataset::simple::Checker writer(f.cfg);

        // Check should reindex the file
        {
            ReporterExpected e;
            e.rescanned.emplace_back("testds", "2007/07.grib1");
            wassert(actual(writer).check(e, true, true));
        }

        // Repack should find nothing to repack
        wassert(actual(writer).repack_clean(true));
        wassert(actual(not files::hasDontpackFlagfile("testds")));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(1, 2));

    // Remove the file from the index
    {
        dataset::simple::Checker writer(f.cfg);
        writer.removeFile("2007/07.grib1", false);
    }

    // Repack should delete the files not in index
    {
        dataset::simple::Checker writer(f.cfg);

        ReporterExpected e;
        e.deleted.emplace_back("testds", "2007/07.grib1", "42178 freed");
        wassert(actual(writer).repack(e, true));
    }

    // Query is still ok, but empty
    wassert(f.ensure_localds_clean(0, 0));
});

// Test maintenance scan with missing metadata and summary
add_method("scan_missing_md_summary", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(sys::exists("testds/2007/07-08.grib1"));
            ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(f.cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }

    // Maintenance should show one file to rescan
    {
        simple::Checker writer(f.cfg);
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(writer).maintenance(expected));
    }

    // Fix the dataset
    {
        simple::Checker writer(f.cfg);

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer).check(e, true, true));

        // Repack should do nothing
        wassert(actual(writer).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + f.idxfname()));

    // Repack here should act as if the dataset were empty
    {
        simple::Checker writer(f.cfg);

        // Repack should find nothing to repack
        wassert(actual(writer).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        dataset::simple::Reader reader(f.cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
});

// Test maintenance scan on missing summary
add_method("scan_missing_summary", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(sys::exists("testds/2007/07-08.grib1"));
            ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(f.cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }

    // Maintenance should show one file to rescan
    {
        simple::Checker writer(f.cfg);
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(writer).maintenance(expected));
    }

    // Fix the dataset
    {
        simple::Checker writer(f.cfg);

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer).check(e, true, true));

        // Repack should do nothing
        wassert(actual(writer).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + f.idxfname()));

    // Repack here should act as if the dataset were empty
    {
        simple::Checker writer(f.cfg);
        wassert(actual(writer).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        dataset::simple::Reader reader(f.cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
});

// Test maintenance scan on compressed archives
add_method("scan_compressed", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            // Compress one file
            metadata::Collection mdc;
            mdc.read_from_file("testds/2007/07-08.grib1.metadata");
            ensure_equals(mdc.size(), 1u);
            mdc.compressDataFile(1024, "metadata file testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1");

            ensure(!sys::exists("testds/2007/07-08.grib1"));
            ensure(sys::exists("testds/2007/07-08.grib1.gz"));
            ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
            ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(sys::exists("testds/2007/07-08.grib1.summary"));
        }

        void removemd()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    wassert(f.ensure_localds_clean(3, 3));

	// Try removing summary and metadata
	setup.removemd();

    // Cannot query anymore
    {
        metadata::Collection mdc;
        simple::Reader reader(f.cfg);
        try {
            mdc.add(reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }

    // Maintenance should show one file to rescan
    {
        simple::Checker writer(f.cfg);
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(writer).maintenance(expected));
    }

    // Fix the dataset
    {
        simple::Checker writer(f.cfg);

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer).check(e, true, true));

        // Repack should do nothing
        wassert(actual(writer).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    ensure(!sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
    ensure(sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + f.idxfname()));
    setup.removemd();

    // Repack here should act as if the dataset were empty
    {
        // Repack should find nothing to repack
        simple::Checker writer(f.cfg);
        wassert(actual(writer).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        metadata::Collection mdc;
        simple::Reader reader(f.cfg);
        try {
            mdc.add(reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }
    ensure(!sys::exists("testds/2007/07-08.grib1"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz"));
    ensure(sys::exists("testds/2007/07-08.grib1.gz.idx"));
    ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
});

// Test maintenance scan on dataset with a file indexed but missing
add_method("scan_missingdata", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib1.summary");
            sys::unlink_ifexists("testds/2007/07-08.grib1.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib1");
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        dataset::simple::Reader reader(f.cfg);
        metadata::Collection mdc(reader, Matcher());
        ensure_equals(mdc.size(), 2u);
    }

    // Maintenance should show one file to rescan
    {
        simple::Checker writer(f.cfg);
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_DELETED] = 1;
        wassert(actual(writer).maintenance(expected));
    }

    // Fix the dataset
    {
        simple::Checker writer(f.cfg);

        // Check should reindex the file
        ReporterExpected e;
        e.deindexed.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer).check(e, true, true));

        // Repack should do nothing
        wassert(actual(writer).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(2, 2));
    ensure(sys::exists("testds/" + f.idxfname()));


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + f.idxfname()));

    // Repack here should act as if the dataset were empty
    {
        simple::Checker writer(f.cfg);

        // Repack should tidy up the index
        ReporterExpected e;
        e.deindexed.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer).repack(e, true));
    }

    // And everything else should still be queriable
    wassert(f.ensure_localds_clean(2, 2));
    ensure(sys::exists("testds/" + f.idxfname()));
});

#if 0
// Test handling of empty archive dirs (such as last with everything moved away)
def_test(7)
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
	ensure_dataset_clean(arc, 3, 3);
}
#endif

}

}
