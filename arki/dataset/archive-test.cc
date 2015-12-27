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

    Fixture()
        : orig("inbound/test.grib1")
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

        iotrace::init();
    }

    void archive_clean(const std::string& dir, size_t filecount)
    {
        ArchivesChecker checker(dir);
        wassert(actual(checker).maintenance_clean(filecount));
        //wassert(actual_file(str::joinpath(dir, arcidxfname())).exists());
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
        ArchivesChecker checker("testds");
        f.orig.writeAtomically("testds/.archive/last/test.grib1");
        checker.indexFile(".archive/last/test.grib1", metadata::Collection(f.orig));
        wassert(actual_file("testds/.archive/last/test.grib1").exists());
        wassert(actual_file("testds/.archive/last/test.grib1.metadata").exists());
        wassert(actual_file("testds/.archive/last/test.grib1.summary").exists());
        wassert(actual_file("testds/.archive/last/" + manifest_idx_fname()).exists());

        metadata::Collection mdc("testds/.archive/last/test.grib1.metadata");
        wassert(actual(mdc.size()) == 3u);
        wassert(actual(mdc[0].sourceBlob().filename) == "test.grib1");
    }

    wassert(f.archive_clean("testds", 1));

    // Query
    {
        ArchivesReader reader("testds");
        metadata::Collection res(reader, Matcher());
        metadata::Collection orig("inbound/test.grib1");
        // Results are in the same order as the files that have been indexed
        wassert(actual(res == orig));
    }
});

// Test maintenance scan on non-indexed files
add_method("maintenance_nonindexed", [](Fixture& f) {
    system("cp inbound/test.grib1 testds/.archive/last/");

    // Query now has empty results
    {
        ArchivesReader reader("testds");
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 0u);

        // Maintenance should show one file to index
        ArchivesChecker checker("testds");
        arki::tests::MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_ARC_TO_INDEX] = 1;
        wassert(actual(checker).maintenance(expected));
    }

    // Reindex
    {
        // Checker should reindex
        ArchivesChecker checker("testds");
        LineChecker s;
        s.require_line_contains("testds: rescanned in archive .archive/last/test.grib1");
        s.require_line_contains("testds: 1 file rescanned.");
        wassert(actual(checker).check(s, true, true));

        // Repack should do nothing
        wassert(actual(checker).repack_clean(true));
    }

    // Query now has all data
    {
        ArchivesReader reader("testds");
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

// Test maintenance scan on missing metadata
add_method("maintenance_missing_metadata", [](Fixture& f) {
    {
        ArchivesChecker checker("testds");
        system("cp inbound/test.grib1 testds/.archive/last/");
        checker.indexFile(".archive/last/test.grib1", metadata::Collection(f.orig));
        sys::unlink("testds/.archive/last/test.grib1.metadata");
        sys::unlink("testds/.archive/last/test.grib1.summary");
    }

    // All data can be queried anyway
    {
        ArchivesReader reader("testds");
        metadata::Collection mdc(reader, Matcher());
        wassert(actual(mdc.size()) == 3u);

        // Maintenance should show one file to rescan
        ArchivesChecker checker("testds");
        arki::tests::MaintenanceResults expected(false, 1);
        expected.by_type[DatasetTest::COUNTED_ARC_TO_RESCAN] = 1;
        wassert(actual(checker).maintenance(expected));
    }

    // Rescan
    {
        // Checker should reindex
        ArchivesChecker checker("testds");
        LineChecker s;
        s.require_line_contains("testds: rescanned in archive .archive/last/test.grib1");
        s.require_line_contains("testds: 1 file rescanned.");
        wassert(actual(checker).check(s, true, true));

        // Repack should do nothing
        wassert(actual(checker).repack_clean(true));
    }

    // Query now still has all data
    {
        ArchivesReader reader("testds");
        metadata::Collection mdc(reader, Matcher());
        wassert(actual(mdc.size()) == 3u);
    }
});

// Test handling of empty archive dirs (such as last with everything moved away)
add_method("reader_empty_last", [](Fixture& f) {
    // Import a file in a secondary archive
    {
        ArchivesChecker checker("testds");
        system("cp inbound/test.grib1 testds/.archive/foo/");
        checker.indexFile(".archive/foo/test.grib1", metadata::Collection(f.orig));

        wassert(actual(checker).maintenance_clean(1));
        wassert(actual(checker).repack_clean(true));
    }

    // Query has all data
    {
        ArchivesReader reader("testds");
        metadata::Collection mdc(reader, dataset::DataQuery(Matcher()));
        wassert(actual(mdc.size()) == 3u);
    }
});

#if 0
def_test(18)
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-archived");

    unique_ptr<Reader> ds(Reader::create(scen.cfg));

    // Query summary
    Summary s1;
    ds->query_summary(Matcher::parse(""), s1);

    // Query data and summarise the results
    Summary s2;
    ds->query_data(Matcher(), [&](unique_ptr<Metadata> md) { s2.add(*md); return true; });

    // Verify that the time range of the first summary is what we expect
    unique_ptr<reftime::Period> p = downcast<reftime::Period>(s1.getReferenceTime());
    ensure_equals(p->begin, Time(2010, 9, 1, 0, 0, 0));
    ensure_equals(p->end, Time(2010, 9, 30, 0, 0, 0));

    ensure(s1 == s2);
}

def_test(19)
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-manyarchivestates");

    // If dir.summary exists, don't complain if dir is missing
    {
        unique_ptr<Archive> a(Archive::create(str::joinpath(scen.path, ".archive/offline")));
        MaintenanceCollector c;
        a->maintenance([&](const std::string& relpath, segment::FileState state) { c(relpath, state); });
        ensure(c.isClean());
    }

    // Querying dir uses dir.summary if dir is missing
    {
        unique_ptr<Archive> a(Archive::create(str::joinpath(scen.path, ".archive/offline")));
        Summary s;
        a->query_summary(Matcher::parse(""), s);
        ensure(s.count() > 0);
    }

    // Query the summary of the whole dataset and ensure it also spans .archive/offline
    {
        unique_ptr<Reader> ds(Reader::create(scen.cfg));
        Summary s;
        iotrace::Collector ioc;
        ds->query_summary(Matcher::parse(""), s);

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
def_test(20)
{
    using namespace arki::dataset;

    const test::Scenario& scen = test::Scenario::get("ondisk2-manyarchivestates");

    // Check that archive cache works
    unique_ptr<Reader> d(Reader::create(scen.cfg));
    Summary s;

    // Access the datasets so we don't count manifest reads in the iostats below
    d->query_summary(Matcher(), s);

    // Query once without cache, it should scan all the archives
    sys::unlink_ifexists(str::joinpath(scen.path, ".summaries/archives.summary"));
    {
        iotrace::Collector ioc;
        d->query_summary(Matcher(), s);
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
        d->query_summary(Matcher(), s);
        ensure_equals(ioc.events.size(), 2u);
    }

    // Query without cache and with a reftime bound, it should still scan the datasets
    {
        iotrace::Collector ioc;
        d->query_summary(Matcher::parse("reftime:>=2010-09-10"), s);
        ensure_equals(ioc.events.size(), 6u);
    }
}
#endif


}

}
