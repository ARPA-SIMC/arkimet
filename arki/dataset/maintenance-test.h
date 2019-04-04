#ifndef ARKI_DATASET_MAINENANCE_TEST_H
#define ARKI_DATASET_MAINENANCE_TEST_H

#include "arki/dataset/tests.h"

namespace arki {
namespace dataset {
namespace maintenance_test {

enum SegmentType {
    SEGMENT_CONCAT,
    SEGMENT_DIR,
};

struct Fixture : public arki::tests::DatasetTest
{
    SegmentType segment_type;
    std::string format;
    std::vector<std::string> import_files;
    /// relpath of the segment with two data elements in it
    std::string test_relpath;
    std::string test_relpath_wrongstep;
    /// Size of the first datum in test_relepath
    unsigned test_datum_size;

    Fixture(SegmentType segment_type, const std::string& format, const std::string& cfg_instance=std::string());

    /**
     * Compute the dataset state and assert that it contains `segment_count`
     * segments, and that the segment test_relpath has the given state.
     */
    void state_is(unsigned segment_count, unsigned test_relpath_state);

    /**
     * Compute the dataset state and assert that it contains `segment_count`
     * segments, and that the segment test_relpath has the given state.
     */
    void accurate_state_is(unsigned segment_count, unsigned test_relpath_state);

    void test_setup();
};

struct MaintenanceTest : public arki::tests::FixtureTestCase<Fixture>
{
    SegmentType segment_type;

    template<typename... Args>
    MaintenanceTest(const std::string& name, SegmentType segment_type, Args... args)
        : FixtureTestCase(name, segment_type, std::forward<Args>(args)...), segment_type(segment_type)
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

    /**
     * Return true if this dataset can delete data.
     */
    virtual bool can_delete_data() const = 0;

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

    /**
     * Make the test segment 6Gb long (using filesystem holes), with valid
     * imported data at the beginning and at the end.
     *
     * Only works on concat segments
     */
    void make_hugefile();

    /// Delete the first element in the test segment
    void delete_one_in_segment();

    /// Delete all data in the test segment
    void delete_all_in_segment();

    /// Reset sequence file to 0 on the test segment
    void reset_seqfile();

    void register_tests() override;

    virtual void register_tests_concat();
    virtual void register_tests_dir();

    /// Remove a file or a directory
    static void rm_r(const std::string& pathname);
};

}
}
}

#endif
