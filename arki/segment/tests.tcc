#include "arki/segment/tests.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

namespace arki {
namespace tests {

template<class Segment, class Data>
void SegmentTests<Segment, Data>::register_tests()
{
using namespace arki::utils;

this->add_method("create", [](Fixture& f) {
    wassert_true(Segment::can_store(f.td.format));
    std::string curdir = sys::getcwd();
    std::string relpath = "test." + f.td.format;
    metadata::Collection mds(f.td.mds);
    std::shared_ptr<Segment> checker = Segment::create(curdir, relpath, str::joinpath(curdir, relpath), mds);
    wassert_true(checker->exists_on_disk());
    auto rep = [](const std::string& msg) {
        fprintf(stderr, "CHECK: %s\n", msg.c_str());
    };
    wassert(actual(checker->check(rep, mds, true)) == segment::SEGMENT_OK);
    wassert(actual(checker->check(rep, mds, false)) == segment::SEGMENT_OK);
    Pending p = wcallchecked(checker->repack(curdir, mds));
    wassert(p.commit());
    wassert(actual(checker->remove()) > 0u);
    wassert_false(checker->exists_on_disk());
});

}

}
}
