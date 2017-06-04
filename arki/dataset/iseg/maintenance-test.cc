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

    void require_rescan() override
    {
        touch("testds/2007/07-07.grib.index", 1496167200);
    }

    void register_tests() override;
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();
    register_tests_unaligned();
}

Tests test_iseg_plain("arki_dataset_iseg_maintenance", MaintenanceTest::SEGMENT_CONCAT, "type=iseg\nformat=grib\n");
Tests test_iseg_plain_dir("arki_dataset_iseg_maintenance_dirs", MaintenanceTest::SEGMENT_DIR, "type=iseg\nformat=grib\nsegments=dir\n");

}


