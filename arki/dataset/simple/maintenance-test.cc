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

    void require_rescan() override
    {
        touch("testds/2007/07-07.grib.metadata", 1496167200);
    }

    void register_tests() override;
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();

    add_method("check_empty_metadata", R"(
    - `.metadata` file must not be empty [unaligned]
    )", [](Fixture& f) {
        sys::File mdf("testds/2007/07-07.grib.metadata", O_RDWR);
        mdf.ftruncate(0);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_metadata_timestamp", R"(
    - `.metadata` file must not be older than the data [unaligned]
    )", [&](Fixture& f) {
        touch("testds/2007/07-07.grib.metadata", 1496167200);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_summary_timestamp", R"(
    - `.summary` file must not be older than the `.metadata` file [unaligned]
    )", [&](Fixture& f) {
        touch("testds/2007/07-07.grib.summary", 1496167200);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_manifest_timestamp", R"(
    - `MANIFEST` file must not be older than the `.metadata` file [unaligned]
    )", [&](Fixture& f) {
        time_t manifest_ts;
        if (sys::exists("testds/MANIFEST"))
            manifest_ts = sys::timestamp("testds/MANIFEST");
        else
            manifest_ts = sys::timestamp("testds/index.sqlite");

        touch("testds/2007/07-07.grib.metadata", manifest_ts + 1);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("check_metadata_must_contain_reftimes", R"(
    - metadata in the `.metadata` file must contain reference time elements [corrupted]
    )", [&](Fixture& f) {
        metadata::Collection mds;
        mds.read_from_file("testds/2007/07-07.grib.metadata");
        wassert(actual(mds.size()) == 2u);
        for (auto* md: mds)
            md->unset(TYPE_REFTIME);
        mds.writeAtomically("testds/2007/07-07.grib.metadata");

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_CORRUPTED));
    });

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
}

Tests test_simple_plain("arki_dataset_simple_maintenance_plain", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_simple_maintenance_sqlite", MaintenanceTest::SEGMENT_CONCAT, "type=simple\nindex_type=sqlite");
Tests test_simple_plain_dir("arki_dataset_simple_maintenance_plain_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=plain\nsegments=dir\n");
Tests test_simple_sqlite_dir("arki_dataset_simple_maintenance_sqlite_dirs", MaintenanceTest::SEGMENT_DIR, "type=simple\nindex_type=sqlite\nsegments=dir\n");

}
