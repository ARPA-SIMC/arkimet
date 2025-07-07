#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/simple.h"
#include "arki/dataset/simple/manifest.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <csignal>

using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;
using namespace std::string_literals;

namespace {

using namespace arki::dataset::maintenance_test;

template <typename TestFixture> class CheckTests : public CheckTest<TestFixture>
{
    using CheckTest<TestFixture>::CheckTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return true; }
};

template <typename TestFixture> class FixTests : public FixTest<TestFixture>
{
    using FixTest<TestFixture>::FixTest;
    typedef TestFixture Fixture;

    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return true; }
};

template <typename TestFixture>
class RepackTests : public RepackTest<TestFixture>
{
    using RepackTest<TestFixture>::RepackTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return true; }
};

template <typename Fixture> void CheckTests<Fixture>::register_tests()
{
    CheckTest<Fixture>::register_tests();

    this->add_method("check_empty_metadata", R"(
    - an empty `.metadata` file marks a deleted segment [deleted]
    )",
                     [](Fixture& f) {
                         sys::File mdf(
                             "testds" /
                                 sys::with_suffix(f.test_relpath, ".metadata"),
                             O_RDWR);
                         mdf.ftruncate(0);
                         wassert(f.state_is(3, segment::SEGMENT_DELETED));
                     });

    this->add_method(
        "check_metadata_timestamp", R"(
    - `.metadata` file must not be older than the data [unaligned]
    )",
        [&](Fixture& f) {
            sys::touch("testds" / sys::with_suffix(f.test_relpath, ".metadata"),
                       1496167200);
            wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
        });

    this->add_method(
        "check_summary_timestamp", R"(
    - `.summary` file must not be older than the `.metadata` file [unaligned]
    )",
        [&](Fixture& f) {
            sys::touch("testds" / sys::with_suffix(f.test_relpath, ".summary"),
                       1496167200);
            wassert(f.state_is(3, segment::SEGMENT_UNOPTIMIZED));
        });

    this->add_method(
        "check_manifest_timestamp", R"(
    - `MANIFEST` file must not be older than the `.metadata` file [unaligned]
    )",
        [&](Fixture& f) {
            time_t manifest_ts;
            if (std::filesystem::exists("testds/MANIFEST"))
                manifest_ts = sys::timestamp("testds/MANIFEST");
            else
                manifest_ts = sys::timestamp("testds/index.sqlite");

            sys::touch("testds" / sys::with_suffix(f.test_relpath, ".metadata"),
                       manifest_ts + 1);

            wassert(f.state_is(3, segment::SEGMENT_UNOPTIMIZED));
        });

    this->add_method("check_missing_index", R"(
        - if the index has been deleted, accessing the dataset recreates it
          empty, and a check will rebuild it. Until it gets rebuilt, segments
          not present in the index would not be considered when querying the
          dataset
    )",
                     [&](Fixture& f) {
                         wassert(f.query_results({1, 3, 0, 2}));

                         std::filesystem::remove("testds/index.sqlite");
                         std::filesystem::remove("testds/MANIFEST");

                         wassert(f.query_results({}));

                         wassert(f.state_is(3, segment::SEGMENT_UNOPTIMIZED));
                     });

    this->add_method("check_missing_index_spurious_files", R"(
    )",
                     [&](Fixture& f) {
                         wassert(f.query_results({1, 3, 0, 2}));

                         std::filesystem::remove("testds/index.sqlite");
                         std::filesystem::remove("testds/MANIFEST");
                         sys::write_file(std::filesystem::path("testds/2007") /
                                             ("11-11."s + f.format + ".tmp"),
                                         f.format + " GARBAGE 7777");

                         wassert(f.query_results({}));

                         wassert(f.state_is(3, segment::SEGMENT_UNOPTIMIZED));
                     });

    this->add_method(
        "check_metadata_must_contain_reftimes", R"(
    - metadata in the `.metadata` file must contain reference time elements [corrupted]
    )",
        [&](Fixture& f) {
            auto mdpath =
                "testds" / sys::with_suffix(f.test_relpath, ".metadata");
            metadata::Collection mds;
            mds.read_from_file(mdpath);
            wassert(actual(mds.size()) == 2u);
            for (auto& md : mds)
                md->unset(TYPE_REFTIME);

            {
                utils::files::PreserveFileTimes pfmd(mdpath);
                utils::files::PreserveFileTimes pfma("testds/MANIFEST");
                mds.writeAtomically(mdpath);

                simple::manifest::Writer writer("testds", false);
                writer.reread();
                if (const auto* seg = writer.segment(f.test_relpath))
                {
                    writer.set(f.test_relpath, seg->mtime, core::Interval());
                    writer.flush();
                }
            }

            wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        });
}

template <typename Fixture> void RepackTests<Fixture>::register_tests()
{
    RepackTest<Fixture>::register_tests();

    this->add_method(
        "repack_unaligned", R"(
        - [unaligned] segments are not touched, to prevent deleting data that
          should be reindexed instead
    )",
        [&](Fixture& f) {
            f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);

            {
                auto writer(f.makeSegmentedChecker());
                ReporterExpected e;
                e.report.emplace_back("testds", "repack", "2 files ok");
                wassert(actual(writer.get()).repack(e, true));
            }

            wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
        });
}

CheckTests<FixtureConcat>
    test_simple_check_grib("arki_dataset_simple_check_grib", "grib",
                           "type=simple");
CheckTests<FixtureDir>
    test_simple_check_grib_dir("arki_dataset_simple_check_grib_dirs", "grib",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip>
    test_simple_check_grib_zip("arki_dataset_simple_check_grib_zip", "grib",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureConcat>
    test_simple_check_bufr("arki_dataset_simple_check_bufr", "bufr",
                           "type=simple");
CheckTests<FixtureDir>
    test_simple_check_bufr_dir("arki_dataset_simple_check_bufr_dirs", "bufr",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip>
    test_simple_check_bufr_zip("arki_dataset_simple_check_bufr_zip", "bufr",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureConcat> test_simple_check_vm2("arki_dataset_simple_check_vm2",
                                                "vm2", "type=simple");
CheckTests<FixtureDir>
    test_simple_check_vm2_dir("arki_dataset_simple_check_vm2_dirs", "vm2",
                              "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip>
    test_simple_check_vm2_zip("arki_dataset_simple_check_vm2_zip", "vm2",
                              "type=simple", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureDir>
    test_simple_check_odimh5_dir("arki_dataset_simple_check_odimh5", "odimh5",
                                 "type=simple");
CheckTests<FixtureZip>
    test_simple_check_odimh5_zip("arki_dataset_simple_check_odimh5_zip",
                                 "odimh5", "type=simple");

FixTests<FixtureConcat> test_simple_fix_grib("arki_dataset_simple_fix_grib",
                                             "grib", "type=simple");
FixTests<FixtureDir>
    test_simple_fix_grib_dir("arki_dataset_simple_fix_grib_dirs", "grib",
                             "type=simple", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip>
    test_simple_fix_grib_zip("arki_dataset_simple_fix_grib_zip", "grib",
                             "type=simple", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureConcat> test_simple_fix_bufr("arki_dataset_simple_fix_bufr",
                                             "bufr", "type=simple");
FixTests<FixtureDir>
    test_simple_fix_bufr_dir("arki_dataset_simple_fix_bufr_dirs", "bufr",
                             "type=simple", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip>
    test_simple_fix_bufr_zip("arki_dataset_simple_fix_bufr_zip", "bufr",
                             "type=simple", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureConcat> test_simple_fix_vm2("arki_dataset_simple_fix_vm2",
                                            "vm2", "type=simple");
FixTests<FixtureDir> test_simple_fix_vm2_dir("arki_dataset_simple_fix_vm2_dirs",
                                             "vm2", "type=simple",
                                             DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip> test_simple_fix_vm2_zip("arki_dataset_simple_fix_vm2_zip",
                                             "vm2", "type=simple",
                                             DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureDir>
    test_simple_fix_odimh5_dir("arki_dataset_simple_fix_odimh5", "odimh5",
                               "type=simple");
FixTests<FixtureZip>
    test_simple_fix_odimh5_zip("arki_dataset_simple_fix_odimh5_zip", "odimh5",
                               "type=simple");

RepackTests<FixtureConcat>
    test_simple_repack_grib("arki_dataset_simple_repack_grib", "grib",
                            "type=simple");
RepackTests<FixtureDir>
    test_simple_repack_grib_dir("arki_dataset_simple_repack_grib_dirs", "grib",
                                "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip>
    test_simple_repack_grib_zip("arki_dataset_simple_repack_grib_zip", "grib",
                                "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureConcat>
    test_simple_repack_bufr("arki_dataset_simple_repack_bufr", "bufr",
                            "type=simple");
RepackTests<FixtureDir>
    test_simple_repack_bufr_dir("arki_dataset_simple_repack_bufr_dirs", "bufr",
                                "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip>
    test_simple_repack_bufr_zip("arki_dataset_simple_repack_bufr_zip", "bufr",
                                "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureConcat>
    test_simple_repack_vm2("arki_dataset_simple_repack_vm2", "vm2",
                           "type=simple");
RepackTests<FixtureDir>
    test_simple_repack_vm2_dir("arki_dataset_simple_repack_vm2_dirs", "vm2",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip>
    test_simple_repack_vm2_zip("arki_dataset_simple_repack_vm2_zip", "vm2",
                               "type=simple", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureDir>
    test_simple_repack_odimh5_dir("arki_dataset_simple_repack_odimh5", "odimh5",
                                  "type=simple");
RepackTests<FixtureZip>
    test_simple_repack_odimh5_zip("arki_dataset_simple_repack_odimh5_zip",
                                  "odimh5", "type=simple");

} // namespace
