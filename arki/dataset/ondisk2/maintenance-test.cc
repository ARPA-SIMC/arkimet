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

template<typename TestFixture>
class Tests : public MaintenanceTest<TestFixture>
{
    using MaintenanceTest<TestFixture>::MaintenanceTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    /**
     * ondisk2 datasets can store overlaps even on dir segments, because the
     * index sadly lacks a unique constraint on (file, offset)
     */
    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return true; }
};

template<typename TestFixture>
void Tests<TestFixture>::register_tests()
{
    MaintenanceTest<TestFixture>::register_tests();

    this->add_method("check_missing_index", R"(
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

        // A writer currently does not create the index until a write actually happens
        f.makeSegmentedWriter();
        wassert(actual_file("testds/index.sqlite").not_exists());
        wassert(actual_file("testds/needs-check-do-not-pack").exists());

        //sys::unlink("testds/index.sqlite");
        sys::unlink("testds/needs-check-do-not-pack");

        f.makeSegmentedChecker();
        wassert(actual_file("testds/index.sqlite").exists());
        wassert(actual_file("testds/needs-check-do-not-pack").exists());

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_missing_index_spurious_files", R"(
    )", [&](Fixture& f) {
        wassert(f.query_results({1, 3, 0, 2}));

        sys::unlink_ifexists("testds/index.sqlite");
        sys::write_file("testds/2007/11-11." + f.format + ".tmp", f.format + " GARBAGE 7777");

        wassert(f.query_results({}));

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_missing_index_rescan_partial_files", R"(
        - while `needs-check-do-not-pack` is present, files with gaps are
          marked for rescanning instead of repacking. This prevents a scenario
          in which, after the index has been deleted, and some data has been
          imported that got appended to an existing segment, that segment would
          be considered as needing repack instead of rescan. [unaligned]
    )", [&](Fixture& f) {
        // Remove the index and make it as if the second datum in
        // 2007/07-07.grib has never been imported
        sys::unlink("testds/index.sqlite");
        if (sys::isdir("testds/" + f.test_relpath))
            sys::unlink("testds/" + f.test_relpath + "/000001." + f.format);
        else {
            sys::File df("testds/" + f.test_relpath, O_RDWR);
            df.ftruncate(f.test_datum_size);
        }

        // Import the second datum of 2007/07-07.grib again
        {
            auto w = f.makeSegmentedWriter();
            wassert(actual(*w).import(f.import_results[3]));
        }

        // Make sure that the segment is seen as unaligned instead of dirty
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("repack_unaligned", R"(
        - [unaligned] when `needs-check-do-not-pack` is present in the dataset
          root directory, running a repack fails asking to run a check first,
          to prevent deleting data that should be reindexed instead
    )", [&](Fixture& f) {
        f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);

        try {
            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            wassert(actual(writer.get()).repack(e, true));
            wassert(throw std::runtime_error("this should have thrown"));
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("dataset needs checking first"));
        }

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });
}

Tests<FixtureConcat> test_ondisk2_plain_grib("arki_dataset_ondisk2_maintenance_grib", "grib", "type=ondisk2\n");
Tests<FixtureDir> test_ondisk2_plain_grib_dir("arki_dataset_ondisk2_maintenance_grib_dirs", "grib", "type=ondisk2\nsegments=dir\n");
Tests<FixtureConcat> test_ondisk2_plain_bufr("arki_dataset_ondisk2_maintenance_bufr", "bufr", "type=ondisk2\n");
Tests<FixtureDir> test_ondisk2_plain_bufr_dir("arki_dataset_ondisk2_maintenance_bufr_dirs", "bufr", "type=ondisk2\nsegments=dir\n");
Tests<FixtureConcat> test_ondisk2_plain_vm2("arki_dataset_ondisk2_maintenance_vm2", "vm2", "type=ondisk2\n");
Tests<FixtureDir> test_ondisk2_plain_vm2_dir("arki_dataset_ondisk2_maintenance_vm2_dirs", "vm2", "type=ondisk2\nsegments=dir\n");
Tests<FixtureDir> test_ondisk2_plain_odimh5_dir("arki_dataset_ondisk2_maintenance_odimh5", "odimh5", "type=ondisk2\n");

}

