#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

namespace {

using namespace arki::dataset::maintenance_test;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=iseg
            step=daily
        )");
    }
};

class MaintTests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_iseg_maintenance");


template<typename TestFixture>
class CheckTests : public CheckTest<TestFixture>
{
    using CheckTest<TestFixture>::CheckTest;
    typedef TestFixture Fixture;

    bool can_detect_overlap() const override { return TestFixture::segment_type() != SEGMENT_DIR; }
    bool can_detect_segments_out_of_step() const override { return false; }
    bool can_delete_data() const override { return true; }
};

CheckTests<FixtureConcat> test_iseg_check_grib("arki_dataset_iseg_check_grib", "grib", "type=iseg\nformat=grib\n");
CheckTests<FixtureDir> test_iseg_check_grib_dir("arki_dataset_iseg_check_grib_dirs", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip> test_iseg_check_grib_zip("arki_dataset_iseg_check_grib_zip", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureConcat> test_iseg_check_bufr("arki_dataset_iseg_check_bufr", "bufr", "type=iseg\nformat=bufr\n");
CheckTests<FixtureDir> test_iseg_check_bufr_dir("arki_dataset_iseg_check_bufr_dirs", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip> test_iseg_check_bufr_zip("arki_dataset_iseg_check_bufr_zip", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureConcat> test_iseg_check_vm2("arki_dataset_iseg_check_vm2", "vm2", "type=iseg\nformat=vm2\n");
CheckTests<FixtureDir> test_iseg_check_vm2_dir("arki_dataset_iseg_check_vm2_dirs", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureZip> test_iseg_check_vm2_zip("arki_dataset_iseg_check_vm2_zip", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
CheckTests<FixtureDir> test_iseg_check_odimh5_dir("arki_dataset_iseg_check_odimh5", "odimh5", "type=iseg\nformat=odimh5\n");
CheckTests<FixtureZip> test_iseg_check_odimh5_zip("arki_dataset_iseg_check_odimh5_zip", "odimh5", "type=iseg\nformat=odimh5\n");


template<typename TestFixture>
class FixTests : public FixTest<TestFixture>
{
    using FixTest<TestFixture>::FixTest;
    typedef TestFixture Fixture;

    bool can_detect_overlap() const override { return TestFixture::segment_type() != SEGMENT_DIR; }
    bool can_detect_segments_out_of_step() const override { return false; }
    bool can_delete_data() const override { return true; }
};

FixTests<FixtureConcat> test_iseg_fix_grib("arki_dataset_iseg_fix_grib", "grib", "type=iseg\nformat=grib\n");
FixTests<FixtureDir> test_iseg_fix_grib_dir("arki_dataset_iseg_fix_grib_dirs", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip> test_iseg_fix_grib_zip("arki_dataset_iseg_fix_grib_zip", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureConcat> test_iseg_fix_bufr("arki_dataset_iseg_fix_bufr", "bufr", "type=iseg\nformat=bufr\n");
FixTests<FixtureDir> test_iseg_fix_bufr_dir("arki_dataset_iseg_fix_bufr_dirs", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip> test_iseg_fix_bufr_zip("arki_dataset_iseg_fix_bufr_zip", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureConcat> test_iseg_fix_vm2("arki_dataset_iseg_fix_vm2", "vm2", "type=iseg\nformat=vm2\n");
FixTests<FixtureDir> test_iseg_fix_vm2_dir("arki_dataset_iseg_fix_vm2_dirs", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureZip> test_iseg_fix_vm2_zip("arki_dataset_iseg_fix_vm2_zip", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
FixTests<FixtureDir> test_iseg_fix_odimh5_dir("arki_dataset_iseg_fix_odimh5", "odimh5", "type=iseg\nformat=odimh5\n");
FixTests<FixtureZip> test_iseg_fix_odimh5_zip("arki_dataset_iseg_fix_odimh5_zip", "odimh5", "type=iseg\nformat=odimh5\n");


template<typename TestFixture>
class RepackTests : public RepackTest<TestFixture>
{
    using RepackTest<TestFixture>::RepackTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    bool can_detect_overlap() const override { return TestFixture::segment_type() != SEGMENT_DIR; }
    bool can_detect_segments_out_of_step() const override { return false; }
    bool can_delete_data() const override { return true; }
};

RepackTests<FixtureConcat> test_iseg_repack_grib("arki_dataset_iseg_repack_grib", "grib", "type=iseg\nformat=grib\n");
RepackTests<FixtureDir> test_iseg_repack_grib_dir("arki_dataset_iseg_repack_grib_dirs", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip> test_iseg_repack_grib_zip("arki_dataset_iseg_repack_grib_zip", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureConcat> test_iseg_repack_bufr("arki_dataset_iseg_repack_bufr", "bufr", "type=iseg\nformat=bufr\n");
RepackTests<FixtureDir> test_iseg_repack_bufr_dir("arki_dataset_iseg_repack_bufr_dirs", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip> test_iseg_repack_bufr_zip("arki_dataset_iseg_repack_bufr_zip", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureConcat> test_iseg_repack_vm2("arki_dataset_iseg_repack_vm2", "vm2", "type=iseg\nformat=vm2\n");
RepackTests<FixtureDir> test_iseg_repack_vm2_dir("arki_dataset_iseg_repack_vm2_dirs", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureZip> test_iseg_repack_vm2_zip("arki_dataset_iseg_repack_vm2_zip", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
RepackTests<FixtureDir> test_iseg_repack_odimh5_dir("arki_dataset_iseg_repack_odimh5", "odimh5", "type=iseg\nformat=odimh5\n");
RepackTests<FixtureZip> test_iseg_repack_odimh5_zip("arki_dataset_iseg_repack_odimh5_zip", "odimh5", "type=iseg\nformat=odimh5\n");


template<typename TestFixture>
void RepackTests<TestFixture>::register_tests()
{
    RepackTest<TestFixture>::register_tests();

    this->add_method("repack_unaligned", R"(
        - [unaligned] segments are not touched, to prevent deleting data that
          should be reindexed instead
    )", [&](Fixture& f) {
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


void MaintTests::register_tests()
{
    add_method("empty_dir_segment", [&](Fixture& f) {
        // See #279
        f.cfg->set("format", "odimh5");
        sys::makedirs(str::joinpath(f.ds_root, "2021", "11-17.odimh5"));

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "0 files ok");
            wassert(actual(checker.get()).repack(e, true));
        }
    });
}

}


