#include "gzidx.h"
#include "tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>
#include <sstream>

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

Tests<segment::gzidx::Checker, GRIBData> test1("arki_segment_gzidx_grib");
Tests<segment::gzidx::Checker, BUFRData> test2("arki_segment_gzidx_bufr");
Tests<segment::gzidxlines::Checker, VM2Data> test3("arki_segment_gzidxlines_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

}

}

#include "tests.tcc"
