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
        res->append(mdc[i]);
    res->commit();
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
    segment::State state;

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
        wassert(actual(state.value) == segment::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == segment::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == segment::SEGMENT_DIRTY);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[2]);
        mdc1.push_back(mdc[1]);
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == segment::SEGMENT_DIRTY);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == segment::SEGMENT_DIRTY);
    }

    // Simulate corrupted file
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        unique_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 1024;
        mdc1[0].set_source(unique_ptr<types::Source>(src.release()));
        state = segment->check(rep, "test", mdc1);
        wassert(actual(state.value) == segment::SEGMENT_UNALIGNED);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(mdc[0]);
        mdc1.push_back(mdc[1]);
        mdc1.push_back(mdc[2]);
        unique_ptr<types::source::Blob> src(mdc[0].sourceBlob().clone());
        src->offset += 1;
        mdc1[0].set_source(unique_ptr<types::Source>(src.release()));
        state = segment->check(rep, "test", mdc1, false);
        wassert(actual(state.value) == segment::SEGMENT_UNALIGNED);
    }
}

void SegmentRemoveTest::run()
{
    auto segment(make_full_checker());

    wassert(actual(segment->exists_on_disk()).istrue());

    wassert(actual(segment->remove()) >= 44412u);

    wassert(actual(segment->exists_on_disk()).isfalse());
}


void test_append_transaction_ok(segment::Writer* dw, Metadata& md, int append_amount_adjust)
{
    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t data_size = md.data_size();
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    const types::source::Blob& new_source = dw->append(md);
    wassert(actual((size_t)new_source.offset) == orig_fsize);
    wassert(actual((size_t)new_source.size) == data_size);
    wassert(actual(new_source.basedir) == sys::getcwd());
    wassert(actual(new_source.filename) == dw->relname);
    wassert(actual(sys::size(dw->absname)) == orig_fsize + data_size + append_amount_adjust);
    wassert(actual_type(md.source()) == *orig_source);

    // Commit
    dw->commit();

    // After commit, data is appended
    wassert(actual(sys::size(dw->absname)) == orig_fsize + data_size + append_amount_adjust);

    // And metadata is updated
    wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), dw->relname, orig_fsize, data_size));
}

void test_append_transaction_rollback(segment::Writer* dw, Metadata& md, int append_amount_adjust)
{
    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    const types::source::Blob& new_source = dw->append(md);
    wassert(actual((size_t)new_source.offset) == orig_fsize);
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize + new_source.size + append_amount_adjust);
    wassert(actual_type(md.source()) == *orig_source);

    // Rollback
    dw->rollback();

    // After rollback, nothing has changed
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);
}

}
}
