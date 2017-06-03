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

struct MaintenanceTest;

struct SegmentTests
{
    virtual ~SegmentTests();

    /// Truncate segment 2007/07-07.grib
    virtual void truncate_segment() = 0;

    /**
     * Swap the two data in 2007/07-07.grib
     */
    virtual void swap_data() = 0;

    virtual void register_tests(MaintenanceTest& tc);
};


struct MaintenanceTest : public arki::tests::FixtureTestCase<Fixture>
{
    enum SegmentType {
        SEGMENT_CONCAT,
        SEGMENT_DIR,
    };

    SegmentType segment_type;
    SegmentTests* segment_tests = nullptr;

    template<typename... Args>
    MaintenanceTest(const std::string& name, SegmentType segment_type, Args... args)
        : FixtureTestCase(name, std::forward<Args>(args)...), segment_type(segment_type)
    {
        init_segment_tests();
    }
    virtual ~MaintenanceTest();

    void init_segment_tests();

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

    /**
     * Swap the two data in 2007/07-07.grib
     */
    virtual void swap_data() = 0;

    /// Remove 2007/07-07.grib from index
    void deindex();

    /// Make it so that 2007/07-07.grib requires a rescan
    virtual void require_rescan() = 0;

    void register_tests() override;

    /// Rename a file or directory
    static void rename(const std::string& old_pathname, const std::string& new_pathname);

    /// Remove a file or a directory
    static void rm_r(const std::string& pathname);

    /// Set the mtime and atime of a file
    static void touch(const std::string& pathname, time_t ts);
};

}
}
}

#endif
