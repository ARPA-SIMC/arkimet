#ifndef ARKI_DATASET_MAINENANCE_TEST_H
#define ARKI_DATASET_MAINENANCE_TEST_H

#include "arki/dataset/tests.h"

namespace arki {
namespace dataset {
namespace maintenance_test {

struct Fixture : public arki::tests::DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            unique=reftime, origin, product, level, timerange, area
            step=daily
        )");
        import("inbound/mainttest.grib");
    }
};

struct MaintenanceTest : public arki::tests::FixtureTestCase<Fixture>
{
    enum SegmentType {
        SEGMENT_CONCAT,
        SEGMENT_DIR,
    };

    SegmentType segment_type;

    template<typename... Args>
    MaintenanceTest(const std::string& name, SegmentType segment_type, Args... args)
        : FixtureTestCase(name, std::forward<Args>(args)...), segment_type(segment_type)
    {
    }
    virtual ~MaintenanceTest();

    /**
     * Return true if this dataset can represent and detect overlapping data.
     */
    virtual bool can_detect_overlap() const = 0;

    /**
     * Return true if this dataset can deal with segments whose name does not
     * fit the segment step.
     */
    virtual bool can_detect_segments_out_of_step() const = 0;

    std::unique_ptr<dataset::segmented::Checker> checker() { return fixture->makeSegmentedChecker(); }

    /**
     * Move all elements of 2007/07-07.grib forward, leaving a hole at the
     * start.
     */
    void make_hole_start();

    /**
     * Move the second element of 2007/07-07.grib away from the first, leaving
     * a hole.
     */
    void make_hole_middle();

    /**
     * Append some padding to 2007/07-07.grib away from the first, leaving
     * a hole.
     */
    void make_hole_end();

    /**
     * Corrupt the first data in 2007/07-07.grib
     */
    void corrupt_first();

    /// Truncate segment 2007/07-07.grib
    void truncate_segment();

    /// Swap the two data in 2007/07-07.grib
    void swap_data();

    /// Remove 2007/07-07.grib from index
    void deindex();

    /// Make it so that 2007/07-07.grib requires a rescan
    virtual void require_rescan() = 0;

    void register_tests() override;

    virtual void register_tests_concat();
    virtual void register_tests_dir();

    /**
     * Extra tests for datasets that can detect changes in data files
     * potentially not reflected in indices
     */
    virtual void register_tests_unaligned();

    /// Remove a file or a directory
    static void rm_r(const std::string& pathname);

    /// Set the mtime and atime of a file
    static void touch(const std::string& pathname, time_t ts);
};

}
}
}

#endif
