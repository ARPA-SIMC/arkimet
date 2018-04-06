#include "tar.h"
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
} test("arki_dataset_segment_tar");

void Tests::register_tests() {

add_method("check", [] {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .tar segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::tar::Checker::create(root, relname + ".tar", absname + ".tar", mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::tar::Checker(root, relname, absname));
        }
    } test;

    wassert(test.run());
});

add_method("remove", [] {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .tar segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::tar::Checker::create(root, relname + ".tar", absname + ".tar", mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::tar::Checker(root, relname, absname));
        }
    } test;

    wassert(test.run());
});

}

}
