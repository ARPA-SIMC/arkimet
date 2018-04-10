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

class Tests : public MaintenanceTest
{
    using MaintenanceTest::MaintenanceTest;

    void register_tests() override;

    bool can_detect_overlap() const override { return segment_type != SEGMENT_DIR; }
    bool can_detect_segments_out_of_step() const override { return false; }
    bool can_delete_data() const override { return true; }
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();

    add_method("check_new", R"(
        - find data files not known by the index [unaligned]
    )", [&](Fixture& f) {
        remove_index();
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    add_method("check_unaligned", R"(
        - segments found in the dataset without a `.index` file are marked for rescanning [unaligned]
    )", [&](Fixture& f) {
        make_unaligned();
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    add_method("repack_unaligned", R"(
        - [unaligned] segments are not touched
    )", [&](Fixture& f) {
        make_unaligned();

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        wassert(actual(writer.get()).repack(e, true));

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });
}

Tests test_iseg_plain_grib("arki_dataset_iseg_maintenance_grib", MaintenanceTest::SEGMENT_CONCAT, "grib", "type=iseg\nformat=grib\n");
Tests test_iseg_plain_grib_dir("arki_dataset_iseg_maintenance_grib_dirs", MaintenanceTest::SEGMENT_DIR, "grib", "type=iseg\nformat=grib\nsegments=dir\n");
Tests test_iseg_plain_bufr("arki_dataset_iseg_maintenance_bufr", MaintenanceTest::SEGMENT_CONCAT, "bufr", "type=iseg\nformat=bufr\n");
Tests test_iseg_plain_bufr_dir("arki_dataset_iseg_maintenance_bufr_dirs", MaintenanceTest::SEGMENT_DIR, "bufr", "type=iseg\nformat=bufr\nsegments=dir\n");
Tests test_iseg_plain_vm2("arki_dataset_iseg_maintenance_vm2", MaintenanceTest::SEGMENT_CONCAT, "vm2", "type=iseg\nformat=vm2\n");
Tests test_iseg_plain_vm2_dir("arki_dataset_iseg_maintenance_vm2_dirs", MaintenanceTest::SEGMENT_DIR, "vm2", "type=iseg\nformat=vm2\nsegments=dir\n");
Tests test_iseg_plain_odimh5_dir("arki_dataset_iseg_maintenance_odimh5", MaintenanceTest::SEGMENT_DIR, "odimh5", "type=iseg\nformat=odimh5\n");

}


