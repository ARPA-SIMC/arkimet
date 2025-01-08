#include "tests.h"
#include "archive.h"
#include "reporter.h"
#include "simple/writer.h"
#include "arki/query.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/files.h"
#include "arki/iotrace.h"
#include "arki/dataset/session.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    std::shared_ptr<dataset::Session> session;
    core::cfg::Section cfg;
    metadata::TestCollection orig;
    std::shared_ptr<archive::Dataset> config;

    Fixture()
        : orig("inbound/test-sorted.grib1")
    {
    }

    void test_setup()
    {
        session = make_shared<dataset::Session>();

        if (std::filesystem::exists("testds")) sys::rmtree("testds");
        std::filesystem::create_directories("testds/.archive/last");

        cfg.clear();
        cfg.set("path", "testds");
        cfg.set("name", "testds");
        cfg.set("type", "iseg");
        cfg.set("format", "grib");
        cfg.set("step", "daily");
        cfg.set("unique", "origin, reftime");
        cfg.set("archive age", "7");

        config = std::make_shared<archive::Dataset>(session, "testds");

        iotrace::init();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_archive");


void Tests::register_tests() {

add_method("acquire_last", [](Fixture& f) {
    // Acquire
    {
        archive::Checker checker(f.config);
        system("cp -a inbound/test-sorted.grib1 testds/.archive/last/test-sorted.grib1");
        wassert(checker.index_segment("last/test-sorted.grib1", metadata::Collection(f.orig)));
        wassert(actual_file("testds/.archive/last/test-sorted.grib1").exists());
        wassert(actual_file("testds/.archive/last/test-sorted.grib1.metadata").exists());
        wassert(actual_file("testds/.archive/last/test-sorted.grib1.summary").exists());
        wassert(actual_file("testds/.archive/last/" + manifest_idx_fname()).exists());

        metadata::Collection mdc;
        Metadata::read_file("testds/.archive/last/test-sorted.grib1.metadata", mdc.inserter_func());
        wassert(actual(mdc.size()) == 3u);
        wassert(actual(mdc[0].sourceBlob().filename) == "test-sorted.grib1");

        wassert(actual(checker).check_clean(false, true));
    }

    // Query
    {
        archive::Reader reader(f.config);
        metadata::Collection res(reader, Matcher());
        metadata::TestCollection orig("inbound/test-sorted.grib1");
        // Results are in the same order as the files that have been indexed
        wassert(actual(res == orig));
    }
});

// Test maintenance scan on non-indexed files
add_method("maintenance_nonindexed", [](Fixture& f) {
    std::filesystem::create_directories("testds/.archive/last");
    system("cp inbound/test-sorted.grib1 testds/.archive/last/");

    // Query now has empty results
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 0u);

        // Maintenance should show one file to index
        archive::Checker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, false, true));
    }

    // Reindex
    {
        // Checker should reindex
        archive::Checker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, true, true));

        wassert(actual(checker).check_clean(false, true));

        // Repack should do nothing
        wassert(actual(checker).repack_clean(true));
    }

    // Query now has all data
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

// Test handling of empty archive dirs (such as last with everything moved away)
add_method("reader_empty_last", [](Fixture& f) {
    // Create a secondary archive
    {
        metadata::TestCollection mdc("inbound/test-sorted.grib1");
        core::cfg::Section cfg;
        cfg.set("name", "foo");
        cfg.set("path", std::filesystem::weakly_canonical("testds/.archive/foo"));
        cfg.set("type", "simple");
        cfg.set("step", "daily");
        auto writer = f.session->dataset(cfg)->create_writer();
        wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_OK);
        wassert(actual(writer->acquire(mdc[1])) == dataset::ACQ_OK);
        wassert(actual(writer->acquire(mdc[2])) == dataset::ACQ_OK);
        writer->flush();
    }

    // Query has all data
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

// Make sure that subdirectories in archives whose index has disappeared are
// still seen as archives (#91)
add_method("enumerate_no_manifest", [](Fixture& f) {
    // Create a dataset without the manifest
    std::filesystem::create_directories("testds/.archive/last");
    std::filesystem::create_directories("testds/.archive/2007");
    system("cp inbound/test-sorted.grib1 testds/.archive/2007/07.grib");

    // Run check
    {
        archive::Checker checker(f.config);
        wassert(actual(checker.test_count_archives()) == 1u);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.2007", "07.grib");
        wassert(actual(checker).check(e, true, true));
    }

    // Query has all data
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 3u);

        wassert(actual(reader.test_count_archives()) == 1u);
    }

    // Create a .summary file outside the archive, to mark it as offline/readonly
    sys::write_file("testds/.archive/2007.summary", "");

    // Check doesn't find it anymore
    {
        archive::Checker checker(f.config);
        wassert(actual(checker.test_count_archives()) == 0u);
    }

    // Query still has all data
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 3u);

        wassert(actual(reader.test_count_archives()) == 1u);
    }
});

// Test handling of empty archive dirs (such as last with everything moved away)
add_method("reader_offline", [](Fixture& f) {
    system("cp inbound/test.summary testds/.archive/2007.summary");

    // Query has all data
    {
        archive::Reader reader(f.config);
        metadata::Collection mdc(reader, query::Data(Matcher()));
        wassert(actual(mdc.size()) == 0u);
    }
});


}

}
