#include "gz.h"
#include "tests.h"

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
};

Tests<segment::gzconcat::Segment, GRIBData> test1("arki_segment_gz_grib");
Tests<segment::gzconcat::Segment, BUFRData> test2("arki_segment_gz_bufr");
Tests<segment::gzlines::Segment, VM2Data> test3("arki_segment_gzlines_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

}

}

#include "tests.tcc"
