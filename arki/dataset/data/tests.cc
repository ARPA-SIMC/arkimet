#include "tests.h"
#include <arki/scan/any.h>
#include <arki/types/source/blob.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::tests;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset;

namespace arki {
namespace tests {

SegmentTest::SegmentTest()
    : format("grib"), relname("testfile.grib"), absname(sys::fs::abspath(relname))
{
    // Scan the test data
    scan::scan("inbound/test.grib1", mdc);
}

SegmentTest::~SegmentTest()
{
}

auto_ptr<data::Segment> SegmentTest::make_empty_segment()
{
    // Clear potentially existing segments
    system(("rm -rf " + absname).c_str());

    auto_ptr<data::Segment> res(make_segment());
    return res;
}

auto_ptr<data::Segment> SegmentTest::make_full_segment()
{
    auto_ptr<data::Segment> res(make_empty_segment());
    for (unsigned i = 0; i < mdc.size(); ++i)
        res->append(mdc[i]);
    return res;
}

void SegmentCheckTest::run(WIBBLE_TEST_LOCPRM)
{
    auto_ptr<data::Segment> segment(make_full_segment());
    dataset::data::FileState state;

    // A simple segment freshly imported is ok
    state = segment->check(mdc);
    wassert(actual(state.is_ok()).istrue());

    state = segment->check(mdc, true);
    wassert(actual(state.is_ok()).istrue());

    // Simulate one element being deleted
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[1]);
        mdc1.observe(mdc[2]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_PACK);
    }
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[0]);
        mdc1.observe(mdc[2]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_PACK);
    }
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[0]);
        mdc1.observe(mdc[1]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_PACK);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[0]);
        mdc1.observe(mdc[2]);
        mdc1.observe(mdc[1]);
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_PACK);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_PACK);
    }

    // Simulate corrupted file
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[0]);
        mdc1.observe(mdc[1]);
        mdc1.observe(mdc[2]);
        auto_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 3;
        mdc1[0].set_source(auto_ptr<types::Source>(src.release()));
        state = segment->check(mdc1);
        wassert(actual(state.value) == dataset::FILE_TO_RESCAN);
    }
    {
        metadata::Collection mdc1;
        mdc1.observe(mdc[0]);
        mdc1.observe(mdc[1]);
        mdc1.observe(mdc[2]);
        auto_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 3;
        mdc1[0].set_source(auto_ptr<types::Source>(src.release()));
        state = segment->check(mdc1, true);
        wassert(actual(state.value) == dataset::FILE_TO_RESCAN);
    }
}

void SegmentRemoveTest::run(WIBBLE_TEST_LOCPRM)
{
    auto_ptr<data::Segment> segment(make_full_segment());

    wassert(actual(sys::fs::exists(absname)).istrue());

    wassert(actual(segment->remove()) >= 44412);

    wassert(actual(sys::fs::exists(absname)).isfalse());
}

}
}
