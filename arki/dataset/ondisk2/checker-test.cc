#include "arki/dataset/ondisk2/test-utils.h"
#include "arki/dataset/ondisk2/checker.h"
#include "arki/dataset/ondisk2/writer.h"
#include "arki/dataset/ondisk2/reader.h"
#include "arki/dataset/ondisk2/index.h"
#include "arki/dataset/reporter.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/core/file.h"
#include "arki/utils.h"
#include "arki/core/file.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/summary.h"
#include "arki/libconfig.h"
#include "arki/nag.h"
#include <sstream>
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;

namespace std {
static ostream& operator<<(ostream& out, const vector<uint8_t>& buf)
{
    return out.write((const char*)buf.data(), buf.size());
}
}

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            unique = origin, reftime
        )");
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_ondisk2_checker");

void Tests::register_tests() {

// Add here only simple-specific tests that are not convered by tests in dataset-writer-test.cc

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
add_method("reindex_with_duplicates", [](Fixture& f) {
    f.cfg.setValue("step", "monthly");
    GRIBData data;
    sys::makedirs("testds/2007/07");
    // TODO: use segments also in the other tests, and instantiate a new test suite for different segment types
    {
        auto s = f.segments().get_writer("2007/07.grib");
        s->append(data.mds[1]);
        s->append(data.mds[1]);
        s->append(data.mds[0]);
        s->commit();
    }

    {
        auto state = f.scan_state();
        wassert(actual(state.get("testds:2007/07.grib").state) == segment::State(segment::SEGMENT_UNALIGNED));
        wassert(actual(state.size()) == 1u);
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07.grib");
        wassert(actual(*checker).check(e, true, true));
    }

    {
        auto state = f.scan_state();
        wassert(actual(state.get("testds:2007/07.grib").state) == segment::State(segment::SEGMENT_DIRTY));
        wassert(actual(state.size()) == 1u);
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
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        e.repacked.emplace_back("testds", "2007/07.grib", "34960 freed");
        wassert(actual(*checker).repack(e, true));
    }

    wassert(f.ensure_localds_clean(1, 2));

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
        index::WIndex index(f.ondisk2_config());
        index.open();
        index.reset("2007/07-08.grib");
        files::createDontpackFlagfile("testds");
    }

    auto state = f.scan_state();
    wassert(actual(state.count(segment::SEGMENT_OK)) == 2u);
    wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::State(segment::SEGMENT_UNALIGNED));
    wassert(actual(state.size()) == 3u);

    // Perform full maintenance and check that things are still ok afterwards

    // By catting test.grib1 into 07-08.grib, we create 2 metadata that do
    // not fit in that file (1 does).
    // Because they are duplicates of metadata in other files, one cannot
    // use the order of the data in the file to determine which one is the
    // newest. The situation cannot be fixed automatically because it is
    // impossible to determine which of the two duplicates should be thrown
    // away; therefore, we can only interrupt the maintenance and raise an
    // exception calling for manual fixing.
    auto checker = f.makeOndisk2Checker();
    auto e = wassert_throws(std::runtime_error, {
        CheckerConfig opts;
        opts.readonly = false;
        checker->check(opts);
    });
    wassert(actual(e.what()).contains("manual"));
});

add_method("scan_reindex_compressed", [](Fixture& f) {
    f.import();

    // Compress one data file
    {
        metadata::Collection mdc = f.query(Matcher::parse("origin:GRIB1,200"));
        wassert(actual(mdc.size()) == 1u);
        string dest = mdc.ensureContiguousData("metadata file testds/2007/07-08.grib");
        auto checker = f.makeSegmentedChecker();
        checker->segment("2007/07-08.grib")->compress();
    }

    // The dataset should still be clean
    wassert(f.ensure_localds_clean(3, 3));

    // The dataset should still be clean even with an accurate scan
    {
        nag::TestCollect tc;
        wassert(f.ensure_localds_clean(3, 3, true));
    }

    // Remove the index
    system("rm testds/index.sqlite");

    // See how maintenance scan copes
    {
        auto state = f.scan_state();
        wassert(actual(state.count(segment::SEGMENT_UNALIGNED)) == 3u);
        wassert(actual(state.size()) == 3u);

        auto checker = f.makeOndisk2Checker();

        // Perform full maintenance and check that things are still ok afterwards
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-07.grib");
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        e.rescanned.emplace_back("testds", "2007/10-09.grib");
        wassert(actual(*checker).check(e, true, true));
    }
    wassert(f.ensure_localds_clean(3, 3));
    auto checker = f.makeOndisk2Checker();
    wassert(actual(*checker).check_clean(true, true));
    wassert(actual(*checker).repack_clean(true));

    // Ensure that we have the summary cache
    ensure(sys::exists("testds/.summaries/all.summary"));
    ensure(sys::exists("testds/.summaries/2007-07.summary"));
    ensure(sys::exists("testds/.summaries/2007-10.summary"));
});

// Test sanity checks on summary cache
add_method("summary_checks", [](Fixture& f) {
    // If we are root we can always write the summary cache, so the tests
    // will fail
    if (getuid() == 0)
        return;

    f.import();
    files::removeDontpackFlagfile("testds");

    // Dataset is ok
    wassert(f.ensure_localds_clean(3, 3));

    // Perform packing to regenerate the summary cache
    auto checker = f.makeOndisk2Checker();
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

add_method("data_in_right_segment_reindex", [](Fixture& f) {
    f.import();
    metadata::TestCollection mdc("inbound/test.grib1");

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
            checker->segment("2007/10-09.grib")->rescan();
            wassert(throw std::runtime_error("rescan should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Delete index then run a maintenance
    wassert(actual(unlink("testds/index.sqlite")) == 0);

    // Run maintenance check
    {
        auto state = f.scan_state();
        wassert(actual(state.count(segment::SEGMENT_UNALIGNED)) == 3u);
        wassert(actual(state.size()) == 3u);
    }

    {
        // Perform full maintenance and check that things are still ok afterwards
        auto checker = f.makeOndisk2Checker();
        auto e = wassert_throws(std::runtime_error, {
            CheckerConfig opts;
            opts.readonly = false;
            checker->check(opts);
        });
        wassert(actual(e.what()).contains("manual fix is required"));
    }
});

add_method("data_in_right_segment_rescan", [](Fixture& f) {
    f.import();
    metadata::TestCollection mdc("inbound/test.grib1");

    files::createDontpackFlagfile("testds");

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
            checker->segment("2007/06-06.grib")->rescan();
            wassert(throw std::runtime_error("rescanFile should have thrown at this point"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("manual fix is required"));
        }
    }

    // Run maintenance check
    auto state = f.scan_state();
    wassert(actual(state.size()) == 4u);
    wassert(actual(state.count(segment::SEGMENT_OK)) == 3u);
    wassert(actual(state.get("testds:2007/06-06.grib").state) == segment::State(segment::SEGMENT_UNALIGNED));

    {
        // Perform full maintenance and check that things are still ok afterwards
        auto checker = f.makeOndisk2Checker();
        auto e = wassert_throws(std::runtime_error, {
            CheckerConfig opts;
            opts.readonly = false;
            checker->check(opts);
        });
        wassert(actual(e.what()).contains("manual fix is required"));
    }
});

// Test packing a dataset with VM2 data
add_method("pack_vm2", [](Fixture& f) {
#ifndef HAVE_VM2
    throw TestSkipped();
#endif
    core::lock::TestNowait lock_nowait;
    f.clean_and_import("inbound/test.vm2");

    // Take note of all the data and delete every second item
    vector<vector<uint8_t>> orig_data;
    metadata::Collection mdc_imported = f.query(dataset::DataQuery("", true));
    {
        orig_data.reserve(mdc_imported.size());
        for (unsigned i = 0; i < mdc_imported.size(); ++i)
            orig_data.push_back(mdc_imported[i].getData());

        auto writer = f.makeOndisk2Writer();
        for (unsigned i = 0; i < mdc_imported.size(); ++i)
            if (i % 2 == 0)
                writer->remove(mdc_imported[i]);
    }
    for (auto& md: mdc_imported)
        md->unset_source();

    // Ensure the dataset has items to pack
    {
        auto state = f.scan_state();
        wassert(actual(state.get("testds:1987/10-31.vm2").state) == segment::State(segment::SEGMENT_DIRTY));
        wassert(actual(state.get("testds:2011/01-01.vm2").state) == segment::State(segment::SEGMENT_DIRTY));
        wassert(actual(state.count(segment::SEGMENT_DIRTY)) == 2u);
        wassert(actual(state.size()) == 2u);
        wassert(actual_file("testds/.archive").not_exists());
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
    wassert(f.ensure_localds_clean(2, 2));
    wassert(actual_file("testds/.archive").not_exists());

    // Ensure that the data hasn't been corrupted
    metadata::Collection mdc_packed = f.query(dataset::DataQuery("", true));
    wassert(actual(mdc_packed[0]).is_similar(mdc_imported[1]));
    wassert(actual(mdc_packed[1]).is_similar(mdc_imported[3]));
    wassert(actual(mdc_packed[0].getData()) == orig_data[1]);
    wassert(actual(mdc_packed[1].getData()) == orig_data[3]);
});

}

}
