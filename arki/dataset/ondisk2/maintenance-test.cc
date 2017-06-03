#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/simple.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index.h"
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

    void swap_data() override
    {
        throw std::runtime_error("swap_data not yet implemented for ondisk2");
    }

    void require_rescan() override
    {
        throw std::runtime_error("require_rescan not yet implemented for ondisk2");
    }

    void register_tests() override;
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();

#if 0
    add_method("check_metadata_reftimes_must_fit_segment", R"(
    - the span of reference times in each segment must fit inside the interval
      implied by the segment file name (FIXME: should this be disabled for
      archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
    )", [&](Fixture& f) {
        metadata::Collection mds;
        mds.read_from_file("testds/2007/07-07.grib.metadata");
        wassert(actual(mds.size()) == 2u);
        mds[0].set("reftime", "2007-07-06 00:00:00");
        mds.writeAtomically("testds/2007/07-07.grib.metadata");

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_CORRUPTED));
    });

    add_method("check_segment_name_must_fit_step", R"(
    - the segment name must represent an interval matching the dataset step
      (FIXME: should this be disabled for archives, to deal with datasets that had
      a change of step in their lifetime?) [corrupted]
    )", [&](Fixture& f) {
        {
            auto w = f.makeSimpleWriter();
            auto& i = w->test_get_index();
            i.test_rename("2007/07-07.grib", "2007/07.grib");
        }
        this->rename("testds/2007/07-07.grib", "testds/2007/07.grib");
        this->rename("testds/2007/07-07.grib.metadata", "testds/2007/07.grib.metadata");
        this->rename("testds/2007/07-07.grib.summary", "testds/2007/07.grib.summary");

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07.grib").state) == segment::State(SEGMENT_CORRUPTED));
    });
#endif
}

Tests test_ondisk2_plain("arki_dataset_ondisk2_maintenance", MaintenanceTest::SEGMENT_CONCAT, "type=ondisk2\n");
Tests test_ondisk2_plain_dir("arki_dataset_ondisk2_maintenance_dirs", MaintenanceTest::SEGMENT_DIR, "type=ondisk2\nsegments=dir\n");

}

