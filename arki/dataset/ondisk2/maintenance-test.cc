#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"

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
        fixture->makeSegmentedChecker()->test_remove_index("2007/07-07.grib");
        files::createDontpackFlagfile("testds");
    }

    void register_tests() override;

    /**
     * ondisk2 datasets can store overlaps even on dir segments, because the
     * index sadly lacks a unique constraint on (file, offset)
     */
    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return true; }
};

void Tests::register_tests()
{
    MaintenanceTest::register_tests();

    add_method("check_new", R"(
        - data files not known by the index are considered data files whose
          entire content has been removed [deleted]
    )", [&](Fixture& f) {
        remove_index();

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_DELETED));
    });

    add_method("check_missing_index", R"(
        - if the index has been deleted, accessing the dataset recreates it
          empty, and creates a `needs-check-do-not-pack` file in the root of
          the dataset.
    )", [&](Fixture& f) {
        sys::unlink("testds/index.sqlite");
        wassert(actual_file("testds/needs-check-do-not-pack").not_exists());

        // A reader won't touch the dataset and just consider it empty
        f.makeSegmentedReader();
        wassert(actual_file("testds/index.sqlite").not_exists());
        wassert(actual_file("testds/needs-check-do-not-pack").not_exists());

        f.makeSegmentedWriter();
        wassert(actual_file("testds/index.sqlite").exists());
        wassert(actual_file("testds/needs-check-do-not-pack").exists());

        sys::unlink("testds/index.sqlite");
        sys::unlink("testds/needs-check-do-not-pack");

        f.makeSegmentedChecker();
        wassert(actual_file("testds/index.sqlite").exists());
        wassert(actual_file("testds/needs-check-do-not-pack").exists());
    });

    add_method("check_unaligned", R"(
        - if a `needs-check-do-not-pack` file is present, segments not known by
          the index are marked for reindexing instead of deletion [unaligned]
    )", [&](Fixture& f) {
        make_unaligned();

        arki::dataset::NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    add_method("repack_unaligned", R"(
        - [unaligned] when `needs-check-do-not-pack` is present in the dataset
          root directory, running a repack fails asking to run a check first,
          to prevent deleting data that should be reindexed instead
    )", [&](Fixture& f) {
        make_unaligned();

        try {
            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            wassert(actual(writer.get()).repack(e, true));
            wassert(throw std::runtime_error("this should have thrown"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("dataset needs checking first"));
        }

        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });
}

Tests test_ondisk2_plain("arki_dataset_ondisk2_maintenance", MaintenanceTest::SEGMENT_CONCAT, "type=ondisk2\n");
Tests test_ondisk2_plain_dir("arki_dataset_ondisk2_maintenance_dirs", MaintenanceTest::SEGMENT_DIR, "type=ondisk2\nsegments=dir\n");

}

