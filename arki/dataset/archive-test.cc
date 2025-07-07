#include "archive.h"
#include "arki/dataset/session.h"
#include "arki/iotrace.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/query.h"
#include "arki/summary.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "reporter.h"
#include "simple/writer.h"
#include "tests.h"

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
    metadata::TestCollection orig;
    std::shared_ptr<archive::Dataset> config;

    Fixture() : orig("inbound/test-sorted.grib1") {}

    void test_setup()
    {
        session = make_shared<dataset::Session>();

        if (std::filesystem::exists("testds"))
            sys::rmtree("testds");
        std::filesystem::create_directories("testds/.archive/last");

        config = std::make_shared<archive::Dataset>(session, "testds/.archive",
                                                    "yearly");

        iotrace::init();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_archive");

void Tests::register_tests()
{

    add_method("acquire_last", [](Fixture& f) {
        // Acquire
        {
            archive::Checker checker(f.config);
            std::filesystem::create_directories("testds/.archive/last/20");
            system("cp -a inbound/test-sorted.grib1 "
                   "testds/.archive/last/20/2007.grib");
            wassert(checker.index_segment("last/20/2007.grib",
                                          metadata::Collection(f.orig)));
            wassert(actual_file("testds/.archive/last/20/2007.grib").exists());
            wassert(actual_file("testds/.archive/last/20/2007.grib.metadata")
                        .exists());
            wassert(actual_file("testds/.archive/last/20/2007.grib.summary")
                        .exists());
            wassert(actual_file("testds/.archive/last/MANIFEST").exists());
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
        std::filesystem::create_directories("testds/.archive/last/20");
        system(
            "cp inbound/test-sorted.grib1 testds/.archive/last/20/2007.grib");

        // Query now has empty results
        {
            archive::Reader reader(f.config);
            metadata::Collection mdc(reader, query::Data(Matcher()));
            wassert(actual(mdc.size()) == 0u);

            // Maintenance should show one file to index
            archive::Checker checker(f.config);
            ReporterExpected e;
            e.rescanned.emplace_back("archives.last", "20/2007.grib");
            wassert(actual(checker).check(e, false, true));
        }

        // Reindex
        {
            // Checker should reindex
            archive::Checker checker(f.config);
            ReporterExpected e;
            e.rescanned.emplace_back("archives.last", "20/2007.grib");
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

    // Test maintenance scan on non-indexed files
    add_method("maintenance_nonindexed_outofstep", [](Fixture& f) {
        std::filesystem::create_directories("testds/.archive/last/");
        system("cp inbound/test-sorted.grib1 "
               "testds/.archive/last/test-sorted.grib1");

        // Query now has empty results
        {
            archive::Reader reader(f.config);
            metadata::Collection mdc(reader, query::Data(Matcher()));
            wassert(actual(mdc.size()) == 0u);

            // Maintenance should show one file needing manual intervention
            archive::Checker checker(f.config);
            ReporterExpected e;
            e.segment_manual_intervention.emplace_back("archives.last",
                                                       "test-sorted.grib1");
            wassert(actual(checker).check(e, false, true));
        }
    });

    // Test handling of empty archive dirs (such as last with everything moved
    // away)
    add_method("reader_empty_last", [](Fixture& f) {
        // Create a secondary archive
        {
            metadata::TestCollection mdc("inbound/test-sorted.grib1");
            core::cfg::Section cfg;
            cfg.set("name", "foo");
            cfg.set("path", sys::abspath("testds/.archive/foo"));
            cfg.set("type", "simple");
            cfg.set("step", "daily");
            auto writer = f.session->dataset(cfg)->create_writer();
            wassert(actual(writer).acquire_ok(mdc));
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
        std::filesystem::create_directories("testds/.archive/2007/20/");
        system(
            "cp inbound/test-sorted.grib1 testds/.archive/2007/20/2007.grib");

        // Run check
        {
            archive::Checker checker(f.config);
            wassert(actual(checker.test_count_archives()) == 1u);
            ReporterExpected e;
            e.rescanned.emplace_back("archives.2007", "20/2007.grib");
            wassert(actual(checker).check(e, true, true));
        }

        // Query has all data
        {
            archive::Reader reader(f.config);
            metadata::Collection mdc(reader, query::Data(Matcher()));
            wassert(actual(mdc.size()) == 3u);

            wassert(actual(reader.test_count_archives()) == 1u);
        }

        // Create a .summary file outside the archive, to mark it as
        // offline/readonly
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

    // Test handling of empty archive dirs (such as last with everything moved
    // away)
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

} // namespace
