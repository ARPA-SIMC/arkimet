#include "arki/dataset/tests.h"
#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/reader.h"
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
            type=iseg
            step=daily
            format=grib
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
} test("arki_dataset_iseg_writer");

void Tests::register_tests() {

// Add here only iseg-specific tests that are not convered by tests in dataset-writer-test.cc

// Test acquiring data
add_method("acquire", [](Fixture& f) {
    // Clean the dataset
    f.clean();

    metadata::Collection mdc("inbound/test.grib1");
    Metadata& md = mdc[0];

    auto writer = f.makeIsegWriter();

    // Import once in the empty dataset
    Writer::AcquireResult res = writer->acquire(md);
    wassert(actual(res) == Writer::ACQ_OK);
    #if 0
    for (vector<Note>::const_iterator i = md.notes.begin();
            i != md.notes.end(); ++i)
        cerr << *i << endl;
    #endif
    wassert(actual(dsname(md)) == "testds");

    wassert(actual_type(md.source()).is_source_blob("grib", sys::abspath("./testds"), "2007/07-08.grib", 0, 7218));

    // Import again works fine
    res = writer->acquire(md);
    wassert(actual(res) == Writer::ACQ_ERROR_DUPLICATE);

    // Flush the changes and check that everything is allright
    writer->flush();
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib.index").exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/" + f.idxfname()).not_exists());
    wassert(actual(sys::timestamp("testds/2007/07-08.grib")) <= sys::timestamp("testds/2007/07-08.grib.index"));
    ensure(!files::hasDontpackFlagfile("testds"));

    wassert(f.ensure_localds_clean(1, 1));
});

#if 0

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
add_method("reindex_with_duplicates", [](Fixture& f) {
    f.cfg.setValue("step", "monthly");
    testdata::GRIBData data;
    sys::makedirs("testds/2007/07");
    // TODO: use segments also in the other tests, and instantiate a new test suite for different segment types
    Segment* s = f.segments().get_segment("2007/07.grib");
    s->append(data.test_data[1].md);
    s->append(data.test_data[1].md);
    s->append(data.test_data[0].md);

    auto checker = f.makeOndisk2Checker();
    {
        MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_DELETED] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07.grib");
        wassert(actual(*checker).check(e, true, true));
    }

    {
        MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    wassert(actual(sys::size("testds/2007/07.grib")) == 34960*2u + 7218u);

    {
        // Test querying: reindexing should have chosen the last version of
        // duplicate items
        auto reader = f.makeOndisk2Reader();
        ensure(reader->hasWorkingIndex());
        metadata::Collection mdc(*reader, Matcher::parse("reftime:=2007-07-07"));
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("testds"), "2007/07.grib", 34960, 34960));

        mdc.clear();
        mdc.add(*reader, Matcher::parse("reftime:=2007-07-08"));
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("testds"), "2007/07.grib", 34960*2, 7218));
    }

    // Perform packing and check that things are still ok afterwards
    {
        ReporterExpected e;
        e.repacked.emplace_back("testds", "2007/07.grib", "34960 freed");
        wassert(actual(*checker).repack(e, true));
    }

    wassert(actual(*checker).maintenance_clean(1));

    wassert(actual(sys::size("testds/2007/07.grib")) == 34960u + 7218u);

    // Test querying, and see that things have moved to the beginning
    auto reader = f.makeOndisk2Reader();
    ensure(reader->hasWorkingIndex());
    metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,80"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("testds"), "2007/07.grib", 0, 34960));

    // Query the second element and check that it starts after the first one
    // (there used to be a bug where the rebuild would use the offsets of
    // the metadata instead of the data)
    mdc.clear();
    mdc.add(*reader, Matcher::parse("reftime:=2007-07-08"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("testds"), "2007/07.grib", 34960, 7218));

    // Ensure that we have the summary cache
    ensure(sys::exists("testds/.summaries/all.summary"));
    ensure(sys::exists("testds/.summaries/2007-07.summary"));
    ensure(sys::exists("testds/.summaries/2007-10.summary"));
});

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
add_method("scan_reindex", [](Fixture& f) {
    f.import();
    system("cat inbound/test.grib1 >> testds/2007/07-08.grib");
    {
        index::WContents index(f.ondisk2_config());
        index.open();
        index.reset("2007/07-08.grib");
    }

    auto checker = f.makeOndisk2Checker();
    MaintenanceResults expected(false, 3);
    expected.by_type[DatasetTest::COUNTED_OK] = 2;
    expected.by_type[DatasetTest::COUNTED_DELETED] = 1;
    wassert(actual(*checker).maintenance(expected));

    // Perform full maintenance and check that things are still ok afterwards

    // By catting test.grib1 into 07-08.grib, we create 2 metadata that do
    // not fit in that file (1 does).
    // Because they are duplicates of metadata in other files, one cannot
    // use the order of the data in the file to determine which one is the
    // newest. The situation cannot be fixed automatically because it is
    // impossible to determine which of the two duplicates should be thrown
    // away; therefore, we can only interrupt the maintenance and raise an
    // exception calling for manual fixing.
    try {
        NullReporter r;
        checker->check(r, true, true);
        ensure(false);
    } catch (std::runtime_error) {
        ensure(true);
    } catch (...) {
        ensure(false);
    }
});

add_method("scan_reindex_compressed", [](Fixture& f) {
    f.import();

    // Compress one data file
    {
        metadata::Collection mdc = f.query(Matcher::parse("origin:GRIB1,200"));
        wassert(actual(mdc.size()) == 1u);
        mdc.compressDataFile(1024, "metadata file testds/2007/07-08.grib");
        sys::unlink_ifexists("testds/2007/07-08.grib");
    }

    // The dataset should still be clean
    {
        wassert(actual(*f.makeOndisk2Checker()).maintenance_clean(3));
    }

    // The dataset should still be clean even with an accurate scan
    {
        nag::TestCollect tc;
        wassert(actual(*f.makeOndisk2Checker()).maintenance_clean(3, false));
    }

    // Remove the index
    system("rm testds/index.sqlite");

    // See how maintenance scan copes
    {
        auto checker = f.makeOndisk2Checker();
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_DELETED] = 3;
        wassert(actual(*checker).maintenance(expected));

        // Perform full maintenance and check that things are still ok afterwards
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-07.grib");
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        e.rescanned.emplace_back("testds", "2007/10-09.grib");
        wassert(actual(*checker).check(e, true, true));
        wassert(actual(*checker).maintenance_clean(3));
        wassert(actual(*checker).check_clean(true, true));
        wassert(actual(*checker).repack_clean(true));

        // Ensure that we have the summary cache
        ensure(sys::exists("testds/.summaries/all.summary"));
        ensure(sys::exists("testds/.summaries/2007-07.summary"));
        ensure(sys::exists("testds/.summaries/2007-10.summary"));
    }
});

// Test sanity checks on summary cache
add_method("summary_checks", [](Fixture& f) {
    // If we are root we can always write the summary cache, so the tests
    // will fail
    if (getuid() == 0)
        return;

    f.import();
    files::removeDontpackFlagfile("testds");

    auto checker = f.makeOndisk2Checker();

    // Dataset is ok
    wassert(actual(*checker).maintenance_clean(3));

    // Perform packing to regenerate the summary cache
    wassert(actual(*checker).repack_clean(true));

    // Ensure that we have the summary cache
    ensure(sys::exists("testds/.summaries/all.summary"));
    ensure(sys::exists("testds/.summaries/2007-07.summary"));
    ensure(sys::exists("testds/.summaries/2007-10.summary"));

    // Make one summary cache file not writable
    chmod("testds/.summaries/all.summary", 0400);

    // Perform check and see that we detect it
    {
        ReporterExpected e;
        e.manual_intervention.emplace_back("testds", "check", sys::abspath("testds/.summaries/all.summary") + " is not writable");
        wassert(actual(*checker).check(e, false, true));
    }

    // Fix it
    {
        ReporterExpected e;
        e.manual_intervention.emplace_back("testds", "check", sys::abspath("testds/.summaries/all.summary") + " is not writable");
        e.progress.emplace_back("testds", "check", "rebuilding summary cache");
        wassert(actual(*checker).check(e, true, true));
    }

    // Check again and see that everything is fine
    wassert(actual(*checker).check_clean(false, true));
});

// Test that the summary cache is properly invalidated on import
add_method("summary_invalidate", [](Fixture& f) {
    // Perform maintenance on empty dir, creating an empty summary cache
    {
        auto checker = f.makeOndisk2Checker();
        wassert(actual(*checker).maintenance_clean(0));
    }

    // Query the summary, there should be no data
    {
        auto reader = f.makeOndisk2Reader();
        ensure(reader->hasWorkingIndex());
        Summary s;
        reader->query_summary(Matcher(), s);
        ensure_equals(s.count(), 0u);
    }

    // Acquire files
    f.import();

    // Query the summary again, there should be data
    {
        auto reader = f.makeOndisk2Reader();
        ensure(reader->hasWorkingIndex());
        Summary s;
        reader->query_summary(Matcher(), s);
        ensure_equals(s.count(), 3u);
    }
});

// Try to reproduce a bug where two conflicting BUFR files were not properly
// imported with USN handling
add_method("regression_0", [](Fixture& f) {
#ifdef HAVE_DBALLE
    ConfigFile cfg;
    cfg.setValue("path", "gts_temp");
    cfg.setValue("name", "gts_temp");
    cfg.setValue("type", "ondisk2");
    cfg.setValue("step", "daily");
    cfg.setValue("unique", "origin, reftime, proddef");
    cfg.setValue("filter", "product:BUFR:t=temp");
    cfg.setValue("replace", "USN");

    auto config = dataset::Config::create(cfg);
    auto writer = config->create_writer();

    Metadata md;
    scan::Bufr scanner;
    scanner.open("inbound/conflicting-temp-same-usn.bufr");
    size_t count = 0;
    for ( ; scanner.next(md); ++count)
        wassert(actual(writer->acquire(md)) == Writer::ACQ_OK);
    wassert(actual(count) == 2u);
    writer->flush();
#endif
});

add_method("data_in_right_segment_reindex", [](Fixture& f) {
    f.import();
    metadata::Collection mdc("inbound/test.grib1");

    // Append one of the GRIBs to the wrong file
    const auto& buf = mdc[1].getData();
    wassert(actual(buf.size()) == 34960u);
    FILE* fd = fopen("testds/2007/10-09.grib", "ab");
    wassert(actual(fd != NULL).istrue());
    wassert(actual(fwrite(buf.data(), buf.size(), 1, fd)) == 1u);
    wassert(actual(fclose(fd)) == 0);

    // A simple rescanFile throws "manual fix is required" error
    {
        auto checker = f.makeOndisk2Checker();
        try {
            checker->rescanSegment("2007/10-09.grib");
            wassert(throw std::runtime_error("rescanFile should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Delete index then run a maintenance
    wassert(actual(unlink("testds/index.sqlite")) == 0);

    // Run maintenance check
    {
        arki::tests::MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_DELETED] = 3;
        wassert(actual(*f.makeOndisk2Checker()).maintenance(expected));
    }

    {
        // Perform full maintenance and check that things are still ok afterwards
        auto checker = f.makeOndisk2Checker();
        try {
            NullReporter r;
            checker->check(r, true, true);
            wassert(throw std::runtime_error("writer.check should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }
});

add_method("data_in_right_segment_rescan", [](Fixture& f) {
    f.import();
    metadata::Collection mdc("inbound/test.grib1");

    // Append one of the GRIBs to the wrong file
    const auto& buf1 = mdc[1].getData();
    const auto& buf2 = mdc[2].getData();
    wassert(actual(buf1.size()) == 34960u);
    wassert(actual(buf2.size()) == 2234u);
    FILE* fd = fopen("testds/2007/06-06.grib", "ab");
    wassert(actual(fd != NULL).istrue());
    wassert(actual(fwrite(buf1.data(), buf1.size(), 1, fd)) == 1u);
    wassert(actual(fwrite(buf2.data(), buf2.size(), 1, fd)) == 1u);
    wassert(actual(fclose(fd)) == 0);

    // A simple rescanFile throws "manual fix is required" error
    {
        auto checker = f.makeOndisk2Checker();
        try {
            checker->rescanSegment("2007/06-06.grib");
            wassert(throw std::runtime_error("rescanFile should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Run maintenance check
    {
        arki::tests::MaintenanceResults expected(false, 4);
        expected.by_type[DatasetTest::COUNTED_OK] = 3;
        expected.by_type[DatasetTest::COUNTED_DELETED] = 1;
        wassert(actual(*f.makeOndisk2Checker()).maintenance(expected));
    }

    {
        // Perform full maintenance and check that things are still ok afterwards
        auto checker = f.makeOndisk2Checker();
        try {
            NullReporter r;
            checker->check(r, true, true);
            wassert(throw std::runtime_error("writer.check should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }
});

// Test packing a dataset with VM2 data
add_method("pack_vm2", [](Fixture& f) {
    f.clean_and_import("inbound/test.vm2");

    // Read everything
    metadata::Collection mdc_imported = f.query(Matcher());

    // Take note of all the data
    vector<vector<uint8_t>> orig_data;
    orig_data.reserve(mdc_imported.size());
    for (unsigned i = 0; i < mdc_imported.size(); ++i)
        orig_data.push_back(mdc_imported[i].getData());

    // Delete every second item
    {
        auto writer = f.makeOndisk2Writer();
        for (unsigned i = 0; i < mdc_imported.size(); ++i)
            if (i % 2 == 0)
                writer->remove(mdc_imported[i]);
    }

    // Ensure the archive has items to pack
    {
        arki::tests::MaintenanceResults expected(false, 2);
        expected.by_type[DatasetTest::COUNTED_DIRTY] = 2;
        wassert(actual(*f.makeOndisk2Checker()).maintenance(expected));

        ensure(!sys::exists("testds/.archive"));
    }

    // Perform packing and check that things are still ok afterwards
    {
        ReporterExpected e;
        e.repacked.emplace_back("testds", "1987/10-31.vm2");
        e.repacked.emplace_back("testds", "2011/01-01.vm2");
        wassert(actual(*f.makeOndisk2Checker()).repack(e, true));
    }

    // Check that the files have actually shrunk
    wassert(actual(sys::stat("testds/1987/10-31.vm2")->st_size) == 36);
    wassert(actual(sys::stat("testds/2011/01-01.vm2")->st_size) == 33);

    // Ensure the archive is now clean
    {
        auto writer(f.makeOndisk2Checker());
        arki::tests::MaintenanceResults expected(true, 2);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        wassert(actual(*writer).maintenance(expected));

        wassert(actual_file("testds/.archive").not_exists());
    }

    // Ensure that the data hasn't been corrupted
    metadata::Collection mdc_packed = f.query(Matcher());
    wassert(actual(mdc_packed[0]).is_similar(mdc_imported[1]));
    wassert(actual(mdc_packed[1]).is_similar(mdc_imported[3]));
    wassert(actual(mdc_packed[0].getData()) == orig_data[1]);
    wassert(actual(mdc_packed[1].getData()) == orig_data[3]);
});

// Test removal of VM2 data
add_method("issue57", [](Fixture& f) {
    f.cfg.setValue("unique", "reftime, area, product");

    // Import the sample file
    sys::write_file("issue57.vm2", "201610050000,12626,139,70,,,000000000\n");
    metadata::Collection input("issue57.vm2");
    {
        auto writer = f.makeOndisk2Writer();
        wassert(actual(writer->acquire(input[0])) == dataset::Writer::ACQ_OK);
    }

    // Query back the data
    metadata::Collection queried(*f.makeOndisk2Reader(), Matcher());
    wassert(actual(queried.size()) == 1);

    // Delete the data
    {
        auto writer = f.makeOndisk2Writer();
        writer->remove(queried[0]);
    }

    // Query back the data
    metadata::Collection after_delete(*f.makeOndisk2Reader(), Matcher());
    wassert(actual(after_delete.size()) == 0);
});

// Test maintenance scan on non-indexed files
add_method("scan_nonindexed", [](Fixture& f) {
    f.cfg.setValue("step", "monthly");

    // Create a new segment, not in the index
    testdata::GRIBData data;
    sys::makedirs("testds/2007");
    // TODO: use segments also in the other tests, and instantiate a new test suite for different segment types
    Segment* s = f.segments().get_segment("2007/07.grib");
    s->append(data.test_data[1].md);
    s->append(data.test_data[0].md);

    // Query now is ok, but empty
    {
        metadata::Collection mdc(*f.makeIsegReader(), Matcher());
        ensure_equals(mdc.size(), 0u);
    }

    // Maintenance should show one file to index
    {
        auto checker = f.makeIsegChecker();
        MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_DELETED] = 1;
        wassert(actual(*checker).maintenance(expected));
        ensure(files::hasDontpackFlagfile("testds"));
    }

    {
        auto checker = f.makeIsegChecker();

        // Check should reindex the file
        {
            ReporterExpected e;
            e.rescanned.emplace_back("testds", "2007/07.grib");
            wassert(actual(*checker).check(e, true, true));
        }

        // Repack should find nothing to repack
        wassert(actual(*checker).repack_clean(true));
        wassert(actual(not files::hasDontpackFlagfile("testds")));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(1, 2));

    // Remove the file from the index
    {
        auto checker = f.makeIsegChecker();
        checker->removeSegment("2007/07.grib", false);
    }

    // Repack should delete the files not in index
    {
        auto checker = f.makeIsegChecker();
        ReporterExpected e;
        e.deleted.emplace_back("testds", "2007/07.grib", "42178 freed");
        wassert(actual(*checker).repack(e, true));
    }

    // Query is still ok, but empty
    wassert(f.ensure_localds_clean(0, 0));
});

// Test maintenance scan with missing metadata and summary
add_method("scan_missing_md_summary", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib.summary");
            ensure(sys::exists("testds/2007/07-08.grib"));
            ensure(!sys::exists("testds/2007/07-08.grib.metadata"));
            ensure(!sys::exists("testds/2007/07-08.grib.summary"));
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        metadata::Collection mdc(*f.makeIsegReader(), Matcher());
        ensure_equals(mdc.size(), 3u);
    }

    // Maintenance should show one file to rescan
    {
        auto checker = f.makeIsegChecker();
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    // Fix the dataset
    {
        auto checker = f.makeIsegChecker();

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

    // Repack should find nothing to repack
    wassert(actual(*f.makeSegmentedChecker()).repack_clean(true));

    // And repack should have changed nothing
    {
        metadata::Collection mdc(*f.makeIsegReader(), Matcher());
        ensure_equals(mdc.size(), 3u);
    }
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(!sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib.summary"));
    ensure(sys::exists("testds/" + f.idxfname()));
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
        metadata::Collection mdc(*f.makeIsegReader(), Matcher());
        ensure_equals(mdc.size(), 3u);
    }

    // Maintenance should show one file to rescan
    {
        auto checker = f.makeIsegChecker();
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    // Fix the dataset
    {
        auto checker = f.makeIsegChecker();

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
    wassert(actual(*f.makeIsegChecker()).repack_clean(true));

    // And repack should have changed nothing
    {
        auto reader = f.makeIsegReader();
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
            mdc.compressDataFile(1024, "metadata file testds/2007/07-08.grib.metadata");
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
        auto reader = f.makeIsegReader();
        try {
            mdc.add(*reader, Matcher());
            ensure(false);
        } catch (std::exception& e) {
            ensure(str::endswith(e.what(), "file needs to be manually decompressed before scanning"));
        }
    }

    // Maintenance should show one file to rescan
    {
        auto checker = f.makeIsegChecker();
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    // Fix the dataset
    {
        auto checker = f.makeIsegChecker();

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
        auto checker = f.makeIsegChecker();
        wassert(actual(*checker).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        metadata::Collection mdc;
        auto reader = f.makeIsegReader();
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

// Test maintenance scan on dataset with a file indexed but missing
add_method("scan_missingdata", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            sys::unlink_ifexists("testds/2007/07-08.grib.summary");
            sys::unlink_ifexists("testds/2007/07-08.grib.metadata");
            sys::unlink_ifexists("testds/2007/07-08.grib");
        }
    } setup;

    f.clean_and_import();
    setup();
    ensure(sys::exists("testds/" + f.idxfname()));

    // Query is ok
    {
        auto reader = f.makeIsegReader();
        metadata::Collection mdc(*reader, Matcher());
        ensure_equals(mdc.size(), 2u);
    }

    // Maintenance should show one file to rescan
    {
        auto checker = f.makeIsegChecker();
        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 2;
        expected.by_type[DatasetTest::COUNTED_MISSING] = 1;
        wassert(actual(*checker).maintenance(expected));
    }

    // Fix the dataset
    {
        auto checker = f.makeIsegChecker();

        // Check should reindex the file
        ReporterExpected e;
        e.deindexed.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).check(e, true, true));

        // Repack should do nothing
        wassert(actual(*checker).repack_clean(true));
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
        // Repack should tidy up the index
        auto checker = f.makeIsegChecker();
        ReporterExpected e;
        e.deindexed.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).repack(e, true));
    }

    // And everything else should still be queriable
    wassert(f.ensure_localds_clean(2, 2));
    ensure(sys::exists("testds/" + f.idxfname()));
});
#endif

#if 0
// Test handling of empty archive dirs (such as last with everything moved away)
def_test(7)
{
    // Import a file in a secondary archive
    {
        system("mkdir testds/.archive/foo");
        Archive arc("testds/.archive/foo");
        arc.openRW();
        system("cp inbound/test.grib testds/.archive/foo/");
        arc.acquire("test.grib");
    }

    // Everything should be fine now
    Archives arc("testds/.archive");
    ensure_dataset_clean(arc, 3, 3);
}
#endif

}

}
