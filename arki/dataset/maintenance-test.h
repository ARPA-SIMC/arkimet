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
    ~MaintenanceTest();

    void init_segment_tests();

    /// Remove a file or a directory
    void rm_r(const std::string& pathname);

    /// Set the mtime and atime of a file
    void touch(const std::string& pathname, time_t ts);

    void register_tests() override;
};

}
}
}

#endif
