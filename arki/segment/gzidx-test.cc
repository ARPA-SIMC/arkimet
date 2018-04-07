#include "gzidx.h"
#include "tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <fcntl.h>
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

template<class Segment, class Data>
struct SegmentFixture : public Fixture
{
    using Fixture::Fixture;
};

template<class Segment, class Data>
class Tests : public FixtureTestCase<SegmentFixture<Segment, Data>>
{
    typedef SegmentFixture<Segment, Data> Fixture;
    using FixtureTestCase<Fixture>::FixtureTestCase;
    void register_tests() override;
};

Tests<segment::gzidx::Checker, GRIBData> test("arki_segment_gzidx");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {

this->add_method("check", [](Fixture& f) {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .gz segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::gzidx::Checker::create(root, relpath, abspath, mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::gzidx::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

this->add_method("remove", [](Fixture& f) {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .gz segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::gzidx::Checker::create(root, relpath, abspath, mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::gzidx::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

}

}

