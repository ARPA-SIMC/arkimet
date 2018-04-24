#include "tar.h"
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

Tests<segment::tar::Checker, GRIBData> test1("arki_segment_tar_grib");
Tests<segment::tar::Checker, BUFRData> test2("arki_segment_tar_bufr");
Tests<segment::tar::Checker, ODIMData> test3("arki_segment_tar_odim");
Tests<segment::tar::Checker, VM2Data>  test4("arki_segment_tar_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

}
}

#include "tests.tcc"
