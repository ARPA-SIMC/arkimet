#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"

namespace {

using namespace arki::dataset::maintenance_test;

class Tests : public MaintenanceTest
{
    using MaintenanceTest::MaintenanceTest;

    void register_tests() override
    {
        MaintenanceTest::register_tests();
    }
};

Tests test_simple_plain("arki_dataset_simple_maintenance_plain", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_simple_maintenance_sqlite", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=sqlite");
Tests test_simple_plain_dir("arki_dataset_simple_maintenance_plain_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=plain\nsegments=dir\n");
Tests test_simple_sqlite_dir("arki_dataset_simple_maintenance_sqlite_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=sqlite\nsegments=dir\n");

}
