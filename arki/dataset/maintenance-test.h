#ifndef ARKI_DATASET_MAINENANCE_TEST_H
#define ARKI_DATASET_MAINENANCE_TEST_H

#include "arki/dataset/tests.h"

namespace arki {
namespace dataset {
namespace maintenance_test {

enum SegmentType {
    SEGMENT_CONCAT,
    SEGMENT_DIR,
    SEGMENT_ZIP,
};

struct Fixture : public arki::tests::DatasetTest
{
    std::string format;
    std::vector<std::filesystem::path> import_files;
    /// relpath of the segment with two data elements in it
    std::filesystem::path test_relpath;
    std::filesystem::path test_relpath_wrongstep;
    /// Size of the first datum in test_relepath
    unsigned test_datum_size;

    Fixture(const std::string& format, const std::string& cfg_instance=std::string(), DatasetTest::TestVariant variant=DatasetTest::TEST_NORMAL);

    static bool segment_can_delete_data() { return true; }
    static bool segment_can_append_data() { return true; }

    /**
     * Return the relative path of test_relpath as found on disk.
     *
     * It can differ from test_relpath in case the segment is archived or compressed
     */
    std::filesystem::path test_relpath_ondisk() const { return test_relpath; }

    /**
     * Compute the dataset state and assert that it contains `segment_count`
     * segments, and that the segment test_relpath has the given state.
     */
    void state_is(unsigned segment_count, const segment::State& test_relpath_state);

    /**
     * Compute the dataset state and assert that it contains `segment_count`
     * segments, and that the segment test_relpath has the given state.
     */
    void accurate_state_is(unsigned segment_count, const segment::State& test_relpath_state);

    void test_setup();

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

    /// Remove the segment from disk
    void remove_segment();
};

struct FixtureConcat : public Fixture
{
    using Fixture::Fixture;

    static SegmentType segment_type() { return SEGMENT_CONCAT; }

    /// Corrupt the dataset so that two data appear to overlap
    void make_overlap();
};

struct FixtureDir : public Fixture
{
    using Fixture::Fixture;

    static SegmentType segment_type() { return SEGMENT_DIR; }

    /// Corrupt the dataset so that two data appear to overlap
    void make_overlap();
};

struct FixtureZip : public Fixture
{
    using Fixture::Fixture;

    static SegmentType segment_type() { return SEGMENT_ZIP; }
    static bool segment_can_delete_data() { return false; }
    static bool segment_can_append_data() { return false; }

    void test_setup();

    std::filesystem::path test_relpath_ondisk() const;
    void remove_segment();

    /// Corrupt the dataset so that two data appear to overlap
    void make_overlap();
};


template<typename TestFixture>
class CheckTest : public arki::tests::FixtureTestCase<TestFixture>
{
public:
    using arki::tests::FixtureTestCase<TestFixture>::FixtureTestCase;
    typedef TestFixture Fixture;

    virtual ~CheckTest();

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

    void register_tests() override;

    virtual void register_tests_concat();
    virtual void register_tests_dir();
    virtual void register_tests_zip();
};

template<typename TestFixture>
class FixTest : public arki::tests::FixtureTestCase<TestFixture>
{
public:
    using arki::tests::FixtureTestCase<TestFixture>::FixtureTestCase;
    typedef TestFixture Fixture;

    virtual ~FixTest();

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

    void register_tests() override;

    virtual void register_tests_concat();
    virtual void register_tests_dir();
};

template<typename TestFixture>
class RepackTest : public arki::tests::FixtureTestCase<TestFixture>
{
public:
    using arki::tests::FixtureTestCase<TestFixture>::FixtureTestCase;
    typedef TestFixture Fixture;

    virtual ~RepackTest();

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

    void register_tests() override;

    virtual void register_tests_concat();
    virtual void register_tests_dir();
};

}
}
}

#endif
