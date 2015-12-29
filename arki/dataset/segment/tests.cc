#include "tests.h"
#include <arki/scan/any.h>
#include <arki/types/source/blob.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset;

namespace arki {
namespace tests {

SegmentTest::SegmentTest()
    : format("grib"), relname("testfile.grib"), absname(sys::abspath(relname))
{
    // Scan the test data
    scan::scan("inbound/test.grib1", mdc.inserter_func());
}

SegmentTest::~SegmentTest()
{
}

unique_ptr<segment::Segment> SegmentTest::make_empty_segment()
{
    // Clear potentially existing segments
    system(("rm -rf " + absname).c_str());

    unique_ptr<segment::Segment> res(make_segment());
    return res;
}

unique_ptr<segment::Segment> SegmentTest::make_full_segment()
{
    unique_ptr<segment::Segment> res(make_empty_segment());
    for (unsigned i = 0; i < mdc.size(); ++i)
    {
        off_t ofs = res->append(mdc[i]);
        mdc[i].set_source(types::Source::createBlob("grib", sys::abspath("."), relname, ofs, mdc[i].data_size()));
    }
    return res;
}

void SegmentCheckTest::run()
{
    unique_ptr<segment::Segment> segment(make_full_segment());
    dataset::segment::State state;

    // A simple segment freshly imported is ok
    state = segment->check(mdc);
    wassert(actual(state.is_ok()).istrue());

    state = segment->check(mdc, true);
    wassert(actual(state.is_ok()).istrue());

    // Simulate one element being deleted
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        mdc1.push_back(mdc[1]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }

    // Simulate corrupted file
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        unique_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 3;
        mdc1[0].set_source(unique_ptr<types::Source>(src.release()));
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_UNALIGNED);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        unique_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 3;
        mdc1[0].set_source(unique_ptr<types::Source>(src.release()));
        state = segment->check(mdc1, true);
        wassert(actual(state.value) == dataset::SEGMENT_UNALIGNED);
    }
}

void SegmentRemoveTest::run()
{
    unique_ptr<segment::Segment> segment(make_full_segment());

    wassert(actual(sys::exists(absname)).istrue());

    wassert(actual(segment->remove()) >= 44412u);

    wassert(actual(sys::exists(absname)).isfalse());
}

}
}
