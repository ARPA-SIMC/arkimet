#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/dataset/tests.h"
#include "arki/dataset/time.h"
#include "arki/matcher/parser.h"
#include "arki/metadata/collection.h"
#include "arki/query.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/files.h"
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

template <class Data> struct FixtureChecker : public DatasetTest
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

template <class Data>
class TestsChecker : public FixtureTestCase<FixtureChecker<Data>>
{
    using FixtureTestCase<FixtureChecker<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsChecker<GRIBData>
    test_checker_grib_simple("arki_dataset_checker_grib_simple", "type=simple");
TestsChecker<GRIBData> test_checker_grib_iseg("arki_dataset_checker_grib_iseg",
                                              "type=iseg\nformat=grib\n");
TestsChecker<BUFRData>
    test_checker_bufr_simple("arki_dataset_checker_bufr_simple", "type=simple");
TestsChecker<BUFRData> test_checker_bufr_iseg("arki_dataset_checker_bufr_iseg",
                                              "type=iseg\nformat=bufr\n");
TestsChecker<VM2Data> test_checker_vm2_simple("arki_dataset_checker_vm2_simple",
                                              "type=simple");
TestsChecker<VM2Data> test_checker_vm2_iseg("arki_dataset_checker_vm2_iseg",
                                            "type=iseg\nformat=vm2\n");
TestsChecker<ODIMData>
    test_checker_odim_simple("arki_dataset_checker_odim_simple", "type=simple");
TestsChecker<ODIMData> test_checker_odim_iseg("arki_dataset_checker_odim_iseg",
                                              "type=iseg\nformat=odimh5\n");
TestsChecker<NCData> test_checker_nc_simple("arki_dataset_checker_nc_simple",
                                            "type=simple");
TestsChecker<NCData> test_checker_nc_iseg("arki_dataset_checker_nc_iseg",
                                          "type=iseg\nformat=nc\n");
TestsChecker<JPEGData>
    test_checker_jpeg_simple("arki_dataset_checker_jpeg_simple", "type=simple");
TestsChecker<JPEGData> test_checker_jpeg_iseg("arki_dataset_checker_jpeg_iseg",
                                              "type=iseg\nformat=jpeg\n");

template <class Data> void TestsChecker<Data>::register_tests()
{

    typedef FixtureChecker<Data> Fixture;

    this->add_method("preconditions", [](Fixture& f) {
        wassert(actual(f.relpaths_old.size()) > 0u);
        wassert(actual(f.relpaths_new.size()) > 0u);
        wassert(actual(f.relpaths_old.size() + f.relpaths_new.size()) ==
                f.count_dataset_files(f.td.mds));
    });

    this->add_method("check", [](Fixture& f) {
        wassert(f.import_all_packed(f.td.mds));

        auto checker(f.makeSegmentedChecker());

        ReporterExpected e;
        e.report.emplace_back("testds", "check", "3 files ok");
        wassert(actual(checker.get()).check(e, true));
    });

    this->add_method("check_archives", [](Fixture& f) {
        auto o = dataset::SessionTime::local_override(
            1184018400); // date +%s --date="2007-07-10"
        wassert(f.import_all(f.td.mds));
        f.cfg->set("archive age", "1");
        f.test_reread_config();

        auto checker(f.makeSegmentedChecker());

        {
            ReporterExpected e;
            e.archived.emplace_back("testds",
                                    "2007/07-07." + format_name(f.td.format));
            e.report.emplace_back("testds", "repack",
                                  "2 files ok, 1 file archived");
            e.report.emplace_back("testds.archives.last", "repack",
                                  "1 file ok");
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
        wassert(
            actual(checker.get())
                .check_filtered(parser.parse("reftime:>=2007-07-08"), e, true));
    });

    this->add_method("remove_all", [](Fixture& f) {
        wassert(f.import_all_packed(f.td.mds));

        {
            auto checker(f.makeSegmentedChecker());

            ReporterExpected e;
            e.deleted.emplace_back("testds",
                                   "2007/07-07." + format_name(f.td.format));
            e.deleted.emplace_back("testds",
                                   "2007/07-08." + format_name(f.td.format));
            e.deleted.emplace_back("testds",
                                   "2007/10-09." + format_name(f.td.format));
            wassert(actual(checker.get()).remove_all(e, true));
        }

        auto state = f.scan_state();
        wassert(actual(state.size()) == 0u);
        wassert(f.query_results({}));

        wassert(actual_file("testds/2007/07-07." + format_name(f.td.format))
                    .not_exists());
        wassert(actual_file("testds/2007/07-08." + format_name(f.td.format))
                    .not_exists());
        wassert(actual_file("testds/2007/10-09." + format_name(f.td.format))
                    .not_exists());
    });

    this->add_method("remove_all_filtered", [](Fixture& f) {
        matcher::Parser parser;
        wassert(f.import_all_packed(f.td.mds));

        {
            auto checker(f.makeSegmentedChecker());

            ReporterExpected e;
            e.deleted.emplace_back("testds",
                                   "2007/07-08." + format_name(f.td.format));
            wassert(actual(checker.get())
                        .remove_all_filtered(
                            parser.parse("reftime:=2007-07-08"), e, true));
        }

        auto state = f.scan_state();
        wassert(actual(state.size()) == 2u);
        wassert(f.query_results({1, 2}));

        wassert(actual_file("testds/2007/07-07." + format_name(f.td.format))
                    .exists());
        wassert(actual_file("testds/2007/07-08." + format_name(f.td.format))
                    .not_exists());
        wassert(actual_file("testds/2007/10-09." + format_name(f.td.format))
                    .exists());
    });

    // Test check_issue51
    this->add_method("check_issue51", [](Fixture& f) {
        f.cfg->set("step", "yearly");
        if (f.td.format != DataFormat::GRIB && f.td.format != DataFormat::BUFR)
            return;
        wassert(f.import_all_packed(f.td.mds));

        // Get metadata for all data in the dataset and corrupt the last
        // character of them all
        metadata::Collection mds = f.query(Matcher());
        wassert(actual(mds.size()) == 3u);
        set<string> destfiles;
        for (const auto& md : mds)
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
            for (const auto& relpath : destfiles)
                e.issue51.emplace_back("testds", relpath,
                                       "segment contains data with corrupted "
                                       "terminator signature");
            wassert(actual(checker.get()).check_issue51(e, false));
        }

        // See if check_issue51 fixes the problem
        {
            ReporterExpected e;
            for (const auto& relpath : destfiles)
                e.issue51.emplace_back("testds", relpath,
                                       "fixed corrupted terminator signatures");
            wassert(actual(checker.get()).check_issue51(e, true));
        }

        // Check that the backup files exist
        for (const auto& relpath : destfiles)
            wassert(
                actual_file(sys::with_suffix(f.local_config()->path / relpath,
                                             ".issue51"))
                    .exists());

        // Do a thorough check to see if everything is ok
        wassert(actual(checker.get()).check_clean(false, false));
    });
}
} // namespace
