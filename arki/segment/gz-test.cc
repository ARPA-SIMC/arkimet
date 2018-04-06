#include "gz.h"
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

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_gz");

void Tests::register_tests() {

add_method("check", [] {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .gz segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::gz::Checker::create(root, relpath, abspath, mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::gz::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

add_method("remove", [] {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .gz segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::gz::Checker::create(root, relpath, abspath, mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::gz::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

}

}

