#include "tests.h"
#include <arki/scan/any.h>
#include <arki/types/source/blob.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/dataset/reporter.h>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset;

namespace arki {
namespace tests {

SegmentTest::SegmentTest()
    : format("grib"), root("."), relname("testfile.grib"), absname(sys::abspath(relname)), mdc("inbound/test.grib1")
{
}

SegmentTest::~SegmentTest()
{
}

std::shared_ptr<segment::Writer> SegmentTest::make_empty_writer()
{
    // Clear potentially existing segments
    system(("rm -rf " + absname).c_str());

    return make_writer();
}

std::shared_ptr<segment::Checker> SegmentTest::make_empty_checker()
{
    // Clear potentially existing segments
    system(("rm -rf " + absname).c_str());

    return make_checker();
}

std::shared_ptr<segment::Writer> SegmentTest::make_full_writer()
{
    auto res(make_empty_writer());
    for (unsigned i = 0; i < mdc.size(); ++i)
        res->append(mdc[i]).commit();
    return res;
}

std::shared_ptr<segment::Checker> SegmentTest::make_full_checker()
{
    make_full_writer();
    return make_checker();
}

void SegmentCheckTest::run()
{
    auto segment(make_full_checker());
    dataset::segment::State state;

    dataset::NullReporter rep;

    // A simple segment freshly imported is ok
    state = segment->check(rep, "test", mdc);
    wassert(actual(state.is_ok()).istrue());

    state = segment->check(rep, "test", mdc, true);
    wassert(actual(state.is_ok()).istrue());

    // Simulate one element being deleted
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        mdc1.push_back(mdc[1]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == dataset::SEGMENT_DIRTY);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        state = segment->check(rep, "test", mdc1);
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
        state = segment->check(rep, "test", mdc1);
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
        state = segment->check(rep, "test", mdc1, true);
        wassert(actual(state.value) == dataset::SEGMENT_UNALIGNED);
    }
}

void SegmentRemoveTest::run()
{
    auto segment(make_full_checker());

    wassert(actual(sys::exists(absname)).istrue());

    wassert(actual(segment->remove()) >= 44412u);

    wassert(actual(sys::exists(absname)).isfalse());
}

}
}
