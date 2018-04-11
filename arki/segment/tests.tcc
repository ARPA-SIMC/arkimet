#include "arki/segment/tests.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types/source/blob.h"
#include <cstring>

namespace arki {
namespace tests {

template<class Segment, class Data>
void SegmentFixture<Segment, Data>::test_setup()
{
    using namespace arki::utils;
    seg_mds = td.mds;
    root = sys::getcwd();
    sys::rmtree_ifexists("testseg");
    sys::mkdir_ifmissing("testseg");
    relpath = "testseg/test." + td.format;
    abspath = sys::abspath(relpath);
}

template<class Segment, class Data>
std::shared_ptr<Segment> SegmentFixture<Segment, Data>::create()
{
    return Segment::create(root, relpath, abspath, seg_mds);
}

template<class Segment, class Data>
void SegmentTests<Segment, Data>::register_tests()
{
using namespace arki::utils;

this->add_method("create", [](Fixture& f) {
    wassert_true(Segment::can_store(f.td.format));
    std::shared_ptr<Segment> checker = f.create();
    wassert_true(checker->exists_on_disk());
});

this->add_method("repack", [](Fixture& f) {
    std::shared_ptr<Segment> checker = f.create();
    Pending p = wcallchecked(checker->repack(f.root, f.seg_mds));
    wassert(p.commit());
    auto rep = [](const std::string& msg) {
        // fprintf(stderr, "POST REPACK %s\n", msg.c_str());
    };
    wassert(actual(checker->check(rep, f.seg_mds)) == segment::SEGMENT_OK);
});

this->add_method("check", [](Fixture& f) {
    std::shared_ptr<Segment> checker = f.create();

    segment::State state;

    auto rep = [](const std::string& msg) {
        // fprintf(stderr, "CHECK %s\n", msg.c_str());
    };

    // A simple segment freshly imported is ok
    wassert(actual(checker->check(rep, f.seg_mds)) == segment::SEGMENT_OK);
    wassert(actual(checker->check(rep, f.seg_mds, true)) == segment::SEGMENT_OK);

    // Simulate one element being deleted
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[2]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate elements out of order
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[2]);
        mdc1.push_back(f.seg_mds[1]);
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate all elements deleted
    {
        metadata::Collection mdc1;
        wassert(actual(checker->check(rep, mdc1)) == segment::SEGMENT_DIRTY);
    }

    // Simulate corrupted file
    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        std::unique_ptr<types::source::Blob> src(f.seg_mds[0].sourceBlob().clone());
        src->offset += 1;
        mdc1[0].set_source(std::unique_ptr<types::Source>(src.release()));
        wassert(actual(checker->check(rep, mdc1, true)) == segment::SEGMENT_CORRUPTED);
        wassert(actual(checker->check(rep, mdc1, false)) == segment::SEGMENT_CORRUPTED);
    }

    {
        metadata::Collection mdc1;
        mdc1.push_back(f.seg_mds[0]);
        mdc1.push_back(f.seg_mds[1]);
        mdc1.push_back(f.seg_mds[2]);
        std::unique_ptr<types::source::Blob> src(f.seg_mds[2].sourceBlob().clone());
        src->offset += 1;
        mdc1[2].set_source(std::unique_ptr<types::Source>(src.release()));
        wassert(actual(checker->check(rep, mdc1, true)) == segment::SEGMENT_CORRUPTED);
        wassert(actual(checker->check(rep, mdc1, false)) == segment::SEGMENT_CORRUPTED);
    }
});

this->add_method("remove", [](Fixture& f) {
    std::shared_ptr<Segment> checker = f.create();
    size_t size = wcallchecked(checker->size());

    wassert(actual(checker->exists_on_disk()).istrue());
    wassert(actual(checker->remove()) == size);
    wassert(actual(checker->exists_on_disk()).isfalse());
});

}

}
}