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

    void register_tests() override;
    void register_segment_tests();
    void register_segment_concat_tests();
    void register_segment_dir_tests();
};

}
}
}

#endif
