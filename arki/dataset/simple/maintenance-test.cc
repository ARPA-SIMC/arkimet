#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/simple.h"
#include "arki/utils/sys.h"

using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

namespace {

using namespace arki::dataset::maintenance_test;

class Tests : public MaintenanceTest
{
    using MaintenanceTest::MaintenanceTest;

    void register_tests() override;
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();

    add_method("check_empty_metadata", R"(
    - `.metadata` file must not be empty
    )", [](Fixture& f) {
        sys::File mdf("testds/2007/07-07.grib.metadata", O_RDWR);
        mdf.ftruncate(0);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_metadata_timestamp", R"(
    - `.metadata` file must not be older than the data
    )", [&](Fixture& f) {
        touch("testds/2007/07-07.grib.metadata", 1496167200);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_metadata_timestamp", R"(
    - `.summary` file must not be older than the `.metadata` file
    )", [&](Fixture& f) {
        touch("testds/2007/07-07.grib.summary", 1496167200);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });
}

Tests test_simple_plain("arki_dataset_simple_maintenance_plain", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_simple_maintenance_sqlite", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=sqlite");
Tests test_simple_plain_dir("arki_dataset_simple_maintenance_plain_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=plain\nsegments=dir\n");
Tests test_simple_sqlite_dir("arki_dataset_simple_maintenance_sqlite_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=sqlite\nsegments=dir\n");

}
