#include "zip.h"
#include "tests.h"
#include "arki/libconfig.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Data, class FixtureData>
class Tests : public SegmentTests<Data, FixtureData>
{
    using SegmentTests<Data, FixtureData>::SegmentTests;
    void register_tests() override;

    void setup() override
    {
        SegmentTests<Data, FixtureData>::setup();
        skip_unless_libzip();
        skip_unless_libarchive();
    }
};

Tests<segment::data::zip::Data, GRIBData> test1("arki_segment_data_zip_grib");
Tests<segment::data::zip::Data, BUFRData> test2("arki_segment_data_zip_bufr");
Tests<segment::data::zip::Data, ODIMData> test3("arki_segment_data_zip_odim");
Tests<segment::data::zip::Data, VM2Data>  test4("arki_segment_data_zip_vm2");
Tests<segment::data::zip::Data, NCData>  test5("arki_segment_data_zip_nc");
Tests<segment::data::zip::Data, JPEGData>  test6("arki_segment_data_zip_jpeg");

template<class Data, class FixtureData>
void Tests<Data, FixtureData>::register_tests() {
SegmentTests<Data, FixtureData>::register_tests();

}
}

#include "tests.tcc"
