#include "arki/dataset/tests.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/reader.h"
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

inline std::string dsname(const Metadata& md)
{
    if (!md.has_source_blob()) return "(md source is not a blob source)";
    return str::basename(md.sourceBlob().basedir);
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
        DatasetTest::clean_and_import(testfile);
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
    f.clean();

    metadata::Collection mdc("inbound/test.grib1");
    Metadata& md = mdc[0];

    auto writer = f.makeSimpleWriter();

    // Import once in the empty dataset
    Writer::AcquireResult res = writer->acquire(md);
    ensure_equals(res, Writer::ACQ_OK);
    #if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
    #endif
    ensure_equals(dsname(md), "testds");

    wassert(actual_type(md.source()).is_source_blob("grib", sys::abspath("./testds"), "2007/07-08.grib", 0, 7218));

    // Import again works fine
    res = writer->acquire(md);
    ensure_equals(res, Writer::ACQ_OK);
    ensure_equals(dsname(md), "testds");

    wassert(actual_type(md.source()).is_source_blob("grib", sys::abspath("./testds"), "2007/07-08.grib", 7218, 7218));

    // Flush the changes and check that everything is allright
    writer->flush();
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
    ensure(sys::timestamp("testds/2007/07-08.grib") <= sys::timestamp("testds/2007/07-08.grib.metadata"));
    ensure(sys::timestamp("testds/2007/07-08.grib.metadata") <= sys::timestamp("testds/2007/07-08.grib.summary"));
    ensure(sys::timestamp("testds/2007/07-08.grib.summary") <= sys::timestamp("testds/" + f.idxfname()));
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
        ensure_equals(dsname(mdc[1]), "testds");
        wassert(actual_type(mdc[1].source()).is_source_blob("grib", sys::abspath("testds"), "20/2007.grib", 34960, 7218));
    }

    ensure(sys::exists("testds/20/2007.grib"));
    ensure(sys::exists("testds/20/2007.grib.metadata"));
    ensure(sys::exists("testds/20/2007.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
    ensure(sys::timestamp("testds/20/2007.grib") <= sys::timestamp("testds/20/2007.grib.metadata"));
    ensure(sys::timestamp("testds/20/2007.grib.metadata") <= sys::timestamp("testds/20/2007.grib.summary"));
    ensure(sys::timestamp("testds/20/2007.grib.summary") <= sys::timestamp("testds/" + f.idxfname()));

    // Dataset is fine and clean
    wassert(f.ensure_localds_clean(1, 2));
});

// Test maintenance scan on missing summary
add_method("scan_missing_summary", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib.summary");
            ensure(sys::exists("testds/2007/07-08.grib"));
            ensure(sys::exists("testds/2007/07-08.grib.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib.summary"));
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        metadata::Collection mdc(*f.makeSimpleReader(), Matcher());
        ensure_equals(mdc.size(), 3u);
    }

    // Maintenance should show one file to rescan
    {
        auto state = f.scan_state();
        wassert(actual(state.get("2007/07-08.grib").state) == segment::State(SEGMENT_UNALIGNED));
        wassert(actual(state.count(SEGMENT_OK)) == 2u);
        wassert(actual(state.count(SEGMENT_UNALIGNED)) == 1u);
        wassert(actual(state.size()) == 3u);
    }

    // Fix the dataset
    {
        auto checker = f.makeSimpleChecker();

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).check(e, true, true));

        // Repack should do nothing
        wassert(actual(*checker).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    ensure(sys::exists("testds/" + f.idxfname()));

    // Repack here should act as if the dataset were empty
    wassert(actual(*f.makeSimpleChecker()).repack_clean(true));

    // And repack should have changed nothing
    {
        auto reader = f.makeSimpleReader();
        metadata::Collection mdc(*reader, Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
});

// Test maintenance scan on compressed archives
add_method("scan_compressed", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            // Compress one file
            metadata::Collection mdc;
            mdc.read_from_file("testds/2007/07-08.grib.metadata");
            ensure_equals(mdc.size(), 1u);
            string dest = mdc.ensureContiguousData("metadata file testds/2007/07-08.grib.metadata");
            scan::compress(dest, 1024);
            sys::unlink_ifexists("testds/2007/07-08.grib");

            ensure(!sys::exists("testds/2007/07-08.grib"));
            ensure(sys::exists("testds/2007/07-08.grib.gz"));
            ensure(sys::exists("testds/2007/07-08.grib.gz.idx"));
            ensure(sys::exists("testds/2007/07-08.grib.metadata"));
            ensure(sys::exists("testds/2007/07-08.grib.summary"));
        }

        void removemd()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib.summary");
            ensure(!sys::exists("testds/2007/07-08.grib.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib.summary"));
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
        auto reader = f.makeSimpleReader();
        try {
            mdc.add(*reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }

    // Maintenance should show one file to rescan
    {
        auto state = f.scan_state();
        wassert(actual(state.get("2007/07-08.grib").state) == segment::State(SEGMENT_UNALIGNED));
        wassert(actual(state.count(SEGMENT_OK)) == 2u);
        wassert(actual(state.count(SEGMENT_UNALIGNED)) == 1u);
        wassert(actual(state.size()) == 3u);
    }

    // Fix the dataset
    {
        auto checker = f.makeSimpleChecker();

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).check(e, true, true));

        // Repack should do nothing
        wassert(actual(*checker).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    ensure(!sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/07-08.grib.gz"));
    ensure(sys::exists("testds/2007/07-08.grib.gz.idx"));
    ensure(sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(sys::exists("testds/2007/07-08.grib.summary"));
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
        auto checker = f.makeSimpleChecker();
        wassert(actual(*checker).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        metadata::Collection mdc;
        auto reader = f.makeSimpleReader();
        try {
            mdc.add(*reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }
    ensure(!sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/07-08.grib.gz"));
    ensure(sys::exists("testds/2007/07-08.grib.gz.idx"));
    ensure(!sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
});

add_method("testacquire", [](Fixture& f) {
    metadata::Collection mdc("inbound/test.grib1");
    stringstream ss;
    wassert(actual(simple::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);

    f.cfg.setValue("archive age", "1");
    wassert(actual(simple::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_ERROR);

    f.cfg.setValue("delete age", "1");
    wassert(actual(simple::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);
});

}

}
