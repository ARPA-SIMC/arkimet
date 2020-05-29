#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/dataset/time.h"
#include "arki/dataset/query.h"
#include "arki/metadata/collection.h"
#include "arki/matcher/parser.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template<class Data>
struct FixtureChecker : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;
    std::set<std::string> relpaths_old;
    std::set<std::string> relpaths_new;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            unique=reftime, origin, product, level, timerange, area
            step=daily
        )");

        // Partition data in two groups: before and after selective_cutoff
        relpaths_old.clear();
        relpaths_new.clear();
        relpaths_old.insert(destfile(td.mds[0]));
        relpaths_old.insert(destfile(td.mds[1]));
        relpaths_new.insert(destfile(td.mds[2]));
    }
};


template<class Data>
class TestsChecker : public FixtureTestCase<FixtureChecker<Data>>
{
    using FixtureTestCase<FixtureChecker<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsChecker<GRIBData> test_checker_grib_ondisk2("arki_dataset_checker_grib_ondisk2", "type=ondisk2\n");
TestsChecker<GRIBData> test_checker_grib_simple_plain("arki_dataset_checker_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<GRIBData> test_checker_grib_simple_sqlite("arki_dataset_checker_grib_simple_sqlite", "type=simple\nindex_type=sqlite\n");
TestsChecker<GRIBData> test_checker_grib_iseg("arki_dataset_checker_grib_iseg", "type=iseg\nformat=grib\n");
TestsChecker<BUFRData> test_checker_bufr_ondisk2("arki_dataset_checker_bufr_ondisk2", "type=ondisk2\n");
TestsChecker<BUFRData> test_checker_bufr_simple_plain("arki_dataset_checker_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<BUFRData> test_checker_bufr_simple_sqlite("arki_dataset_checker_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<BUFRData> test_checker_bufr_iseg("arki_dataset_checker_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsChecker<VM2Data> test_checker_vm2_ondisk2("arki_dataset_checker_vm2_ondisk2", "type=ondisk2\n");
TestsChecker<VM2Data> test_checker_vm2_simple_plain("arki_dataset_checker_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<VM2Data> test_checker_vm2_simple_sqlite("arki_dataset_checker_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<VM2Data> test_checker_vm2_iseg("arki_dataset_checker_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsChecker<ODIMData> test_checker_odim_ondisk2("arki_dataset_checker_odim_ondisk2", "type=ondisk2\n");
TestsChecker<ODIMData> test_checker_odim_simple_plain("arki_dataset_checker_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<ODIMData> test_checker_odim_simple_sqlite("arki_dataset_checker_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<ODIMData> test_checker_odim_iseg("arki_dataset_checker_odim_iseg", "type=iseg\nformat=odimh5\n");

template<class Data>
void TestsChecker<Data>::register_tests() {

typedef FixtureChecker<Data> Fixture;

this->add_method("preconditions", [](Fixture& f) {
    wassert(actual(f.relpaths_old.size()) > 0u);
    wassert(actual(f.relpaths_new.size()) > 0u);
    wassert(actual(f.relpaths_old.size() + f.relpaths_new.size()) == f.count_dataset_files(f.td.mds));
});

this->add_method("check", [](Fixture& f) {
    wassert(f.import_all_packed(f.td.mds));

    auto checker(f.makeSegmentedChecker());

    ReporterExpected e;
    e.report.emplace_back("testds", "check", "3 files ok");
    wassert(actual(checker.get()).check(e, true));
});

this->add_method("check_archives", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    wassert(f.import_all(f.td.mds));
    f.cfg.set("archive age", "1");
    f.test_reread_config();

    auto checker(f.makeSegmentedChecker());

    {
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07." + f.td.format);
        e.report.emplace_back("testds", "repack", "2 files ok, 1 file archived");
        e.report.emplace_back("testds.archives.last", "repack", "1 file ok");
        wassert(actual(*checker).repack(e, true));
    }

    {
        ReporterExpected e(ReporterExpected::ENFORCE_REPORTS);
        e.report.emplace_back("testds", "check", "2 files ok");
        e.report.emplace_back("testds.archives.last", "check", "1 file ok");
        wassert(actual(checker.get()).check(e, true));
    }

    {
        ReporterExpected e(ReporterExpected::ENFORCE_REPORTS);
        e.report.emplace_back("testds", "check", "2 files ok");
        dataset::CheckerConfig opts;
        opts.offline = false;
        wassert(actual(checker.get()).check(e, opts));
    }
});

this->add_method("check_filtered", [](Fixture& f) {
    matcher::Parser parser;
    wassert(f.import_all_packed(f.td.mds));

    auto checker(f.makeSegmentedChecker());

    ReporterExpected e;
    e.report.emplace_back("testds", "check", "2 files ok");
    wassert(actual(checker.get()).check_filtered(parser.parse("reftime:>=2007-07-08"), e, true));
});

this->add_method("remove_all", [](Fixture& f) {
    wassert(f.import_all_packed(f.td.mds));

    {
        auto checker(f.makeSegmentedChecker());

        ReporterExpected e;
        e.deleted.emplace_back("testds", "2007/07-07." + f.td.format);
        e.deleted.emplace_back("testds", "2007/07-08." + f.td.format);
        e.deleted.emplace_back("testds", "2007/10-09." + f.td.format);
        wassert(actual(checker.get()).remove_all(e, true));
    }

    auto state = f.scan_state();
    wassert(actual(state.size()) == 0u);
    wassert(f.query_results({}));

    wassert(actual_file("testds/2007/07-07." + f.td.format).not_exists());
    wassert(actual_file("testds/2007/07-08." + f.td.format).not_exists());
    wassert(actual_file("testds/2007/10-09." + f.td.format).not_exists());
});

this->add_method("remove_all_filtered", [](Fixture& f) {
    matcher::Parser parser;
    wassert(f.import_all_packed(f.td.mds));

    {
        auto checker(f.makeSegmentedChecker());

        ReporterExpected e;
        e.deleted.emplace_back("testds", "2007/07-08." + f.td.format);
        wassert(actual(checker.get()).remove_all_filtered(parser.parse("reftime:=2007-07-08"), e, true));
    }

    auto state = f.scan_state();
    wassert(actual(state.size()) == 2u);
    wassert(f.query_results({1, 2}));

    wassert(actual_file("testds/2007/07-07." + f.td.format).exists());
    wassert(actual_file("testds/2007/07-08." + f.td.format).not_exists());
    wassert(actual_file("testds/2007/10-09." + f.td.format).exists());
});

// Test check_issue51
this->add_method("check_issue51", [](Fixture& f) {
    f.cfg.set("step", "yearly");
    if (f.td.format != "grib" && f.td.format != "bufr") return;
    wassert(f.import_all_packed(f.td.mds));

    // Get metadata for all data in the dataset and corrupt the last character
    // of them all
    metadata::Collection mds = f.query(Matcher());
    wassert(actual(mds.size()) == 3u);
    set<string> destfiles;
    for (const auto& md: mds)
    {
        const auto& blob = md->sourceBlob();
        destfiles.insert(blob.filename);
        utils::files::PreserveFileTimes pt(blob.absolutePathname());
        File f(blob.absolutePathname(), O_RDWR);
        f.lseek(blob.offset + blob.size - 1);
        f.write_all_or_throw("\x0d", 1);
    }

    auto checker(f.config().create_checker());

    // See if check_issue51 finds the problem
    {
        ReporterExpected e;
        for (const auto& relpath: destfiles)
            e.issue51.emplace_back("testds", relpath, "segment contains data with corrupted terminator signature");
        wassert(actual(checker.get()).check_issue51(e, false));
    }

    // See if check_issue51 fixes the problem
    {
        ReporterExpected e;
        for (const auto& relpath: destfiles)
            e.issue51.emplace_back("testds", relpath, "fixed corrupted terminator signatures");
        wassert(actual(checker.get()).check_issue51(e, true));
    }

    // Check that the backup files exist
    for (const auto& relpath: destfiles)
        wassert(actual_file(str::joinpath(f.local_config()->path, relpath) + ".issue51").exists());

    // Do a thorough check to see if everything is ok
    wassert(actual(checker.get()).check_clean(false, false));
});

}
}
