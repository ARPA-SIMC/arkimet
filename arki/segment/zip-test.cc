#include "zip.h"
#include "tests.h"
#include "arki/libconfig.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    void register_tests() override;

    void setup()
    {
        SegmentTests<Segment, Data>::setup();
        skip_unless_libzip();
        skip_unless_libarchive();
    }
};

Tests<segment::zip::Segment, GRIBData> test1("arki_segment_zip_grib");
Tests<segment::zip::Segment, BUFRData> test2("arki_segment_zip_bufr");
Tests<segment::zip::Segment, ODIMData> test3("arki_segment_zip_odim");
Tests<segment::zip::Segment, VM2Data>  test4("arki_segment_zip_vm2");
Tests<segment::zip::Segment, NCData>  test5("arki_segment_zip_nc");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

}
}

#include "tests.tcc"
