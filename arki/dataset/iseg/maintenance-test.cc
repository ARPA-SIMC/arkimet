#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"

using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

namespace {

using namespace arki::dataset::maintenance_test;

template<typename TestFixture>
class Tests : public MaintenanceTest<TestFixture>
{
    using MaintenanceTest<TestFixture>::MaintenanceTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    bool can_detect_overlap() const override { return TestFixture::segment_type() != SEGMENT_DIR; }
    bool can_detect_segments_out_of_step() const override { return false; }
    bool can_delete_data() const override { return true; }
};

template<typename TestFixture>
void Tests<TestFixture>::register_tests()
{
    MaintenanceTest<TestFixture>::register_tests();

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

Tests<FixtureConcat> test_iseg_grib("arki_dataset_iseg_maintenance_grib", "grib", "type=iseg\nformat=grib\n");
Tests<FixtureDir> test_iseg_grib_dir("arki_dataset_iseg_maintenance_grib_dirs", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureZip> test_iseg_grib_zip("arki_dataset_iseg_maintenance_grib_zip", "grib", "type=iseg\nformat=grib\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureConcat> test_iseg_bufr("arki_dataset_iseg_maintenance_bufr", "bufr", "type=iseg\nformat=bufr\n");
Tests<FixtureDir> test_iseg_bufr_dir("arki_dataset_iseg_maintenance_bufr_dirs", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureZip> test_iseg_bufr_zip("arki_dataset_iseg_maintenance_bufr_zip", "bufr", "type=iseg\nformat=bufr\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureConcat> test_iseg_vm2("arki_dataset_iseg_maintenance_vm2", "vm2", "type=iseg\nformat=vm2\n");
Tests<FixtureDir> test_iseg_vm2_dir("arki_dataset_iseg_maintenance_vm2_dirs", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureZip> test_iseg_vm2_zip("arki_dataset_iseg_maintenance_vm2_zip", "vm2", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);
Tests<FixtureDir> test_iseg_odimh5_dir("arki_dataset_iseg_maintenance_odimh5", "odimh5", "type=iseg\nformat=odimh5\n");
Tests<FixtureZip> test_iseg_odimh5_zip("arki_dataset_iseg_maintenance_odimh5_zip", "odimh5", "type=iseg\nformat=odimh5\n");

}


