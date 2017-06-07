#include "tests.h"
#include "archive.h"
#include "simple/writer.h"
#include "test-scenario.h"
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

using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    ConfigFile cfg;
    metadata::Collection orig;
    std::shared_ptr<const ArchivesConfig> config;

    Fixture()
        : orig("inbound/test-sorted.grib1")
    {
    }

    void test_setup()
    {
        if (sys::exists("testds")) sys::rmtree("testds");
        sys::makedirs("testds/.archive/last");

        cfg.clear();
        cfg.setValue("path", "testds");
        cfg.setValue("name", "testds");
        cfg.setValue("type", "ondisk2");
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "origin, reftime");
        cfg.setValue("archive age", "7");

        config = ArchivesConfig::create("testds");

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
        ArchivesChecker checker(f.config);
        system("cp -a inbound/test-sorted.grib1 testds/.archive/last/test-sorted.grib1");
        wassert(checker.indexSegment("last/test-sorted.grib1", metadata::Collection(f.orig)));
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
        ArchivesReader reader(f.config);
        metadata::Collection res(reader, Matcher());
        metadata::Collection orig("inbound/test-sorted.grib1");
        // Results are in the same order as the files that have been indexed
        wassert(actual(res == orig));
    }
});

// Test maintenance scan on non-indexed files
add_method("maintenance_nonindexed", [](Fixture& f) {
    {
#warning TODO: this is a hack to create the datasets before maintenance is called. Replace with a Checker::create() function
        ArchivesChecker checker(f.config);
    }
    system("cp inbound/test-sorted.grib1 testds/.archive/last/");

    // Query now has empty results
    {
        ArchivesReader reader(f.config);
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 0u);

        // Maintenance should show one file to index
        ArchivesChecker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, false, true));
    }

    // Reindex
    {
        // Checker should reindex
        ArchivesChecker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, true, true));

        wassert(actual(checker).check_clean(false, true));

        // Repack should do nothing
        wassert(actual(checker).repack_clean(true));
    }

    // Query now has all data
    {
        ArchivesReader reader(f.config);
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

// Test maintenance scan on missing metadata
add_method("maintenance_missing_metadata", [](Fixture& f) {
    {
        ArchivesChecker checker(f.config);
        system("cp inbound/test-sorted.grib1 testds/.archive/last/");
        wassert(checker.indexSegment("last/test-sorted.grib1", metadata::Collection(f.orig)));
        sys::unlink("testds/.archive/last/test-sorted.grib1.metadata");
        sys::unlink("testds/.archive/last/test-sorted.grib1.summary");
    }

    // All data can be queried anyway
    {
        ArchivesReader reader(f.config);
        metadata::Collection mdc(reader, Matcher());
        wassert(actual(mdc.size()) == 3u);

        // Maintenance should show one file to rescan
        ArchivesChecker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, false, true));
    }

    // Rescan
    {
        // Checker should reindex
        ArchivesChecker checker(f.config);
        ReporterExpected e;
        e.rescanned.emplace_back("archives.last", "test-sorted.grib1");
        wassert(actual(checker).check(e, true, true));

        wassert(actual(checker).check_clean(false, true));

        // Repack should do nothing
        wassert(actual(checker).repack_clean(true));
    }

    // Query now still has all data
    {
        ArchivesReader reader(f.config);
        metadata::Collection mdc(reader, Matcher());
        wassert(actual(mdc.size()) == 3u);
    }
});

// Test handling of empty archive dirs (such as last with everything moved away)
add_method("reader_empty_last", [](Fixture& f) {
    // Create a secondary archive
    {
        ConfigFile cfg;
        cfg.setValue("name", "foo");
        cfg.setValue("path", sys::abspath("testds/.archive/foo"));
        cfg.setValue("type", "simple");
        cfg.setValue("step", "daily");
        auto config = dataset::Config::create(cfg);
        auto writer = config->create_writer();
        writer->flush();
    }

    // Import a file in the secondary archive
    {
        system("cp inbound/test-sorted.grib1 testds/.archive/foo/");

        ArchivesChecker checker(f.config);
        wassert(checker.indexSegment("foo/test-sorted.grib1", metadata::Collection(f.orig)));

        wassert(actual(checker).check_clean(true, true));
        wassert(actual(checker).repack_clean(true));
    }

    // Query has all data
    {
        ArchivesReader reader(f.config);
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

}

}
