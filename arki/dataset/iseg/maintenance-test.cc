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

    void make_unaligned() override
    {
        sys::unlink("testds/2007/07-07.grib.index");
    }

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

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_unaligned", R"(
        - segments found in the dataset without a `.index` file are marked for rescanning [unaligned]
    )", [&](Fixture& f) {
        make_unaligned();

        arki::dataset::NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("repack_unaligned", R"(
        - [unaligned] segments are not touched
    )", [&](Fixture& f) {
        make_unaligned();

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        wassert(actual(writer.get()).repack(e, true));

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });
}

Tests test_iseg_plain("arki_dataset_iseg_maintenance", MaintenanceTest::SEGMENT_CONCAT, "type=iseg\nformat=grib\n");
Tests test_iseg_plain_dir("arki_dataset_iseg_maintenance_dirs", MaintenanceTest::SEGMENT_DIR, "type=iseg\nformat=grib\nsegments=dir\n");

}


