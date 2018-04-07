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

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    void register_tests() override;
};

Tests<segment::tar::Checker, GRIBData> test1("arki_segment_tar_grib");
Tests<segment::tar::Checker, BUFRData> test2("arki_segment_tar_bufr");
Tests<segment::tar::Checker, ODIMData> test3("arki_segment_tar_odim");
Tests<segment::tar::Checker, VM2Data>  test4("arki_segment_tar_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

this->add_method("check", [](Fixture& f) {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .tar segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::tar::Checker::create(root, relpath + ".tar", abspath + ".tar", mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::tar::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

this->add_method("remove", [](Fixture& f) {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            throw std::runtime_error("writes for .tar segments are not implemented");
        }
        std::shared_ptr<segment::Checker> make_full_checker() override
        {
            segment::tar::Checker::create(root, relpath + ".tar", abspath + ".tar", mdc);
            return make_checker();
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::tar::Checker(root, relpath, abspath));
        }
    } test;

    wassert(test.run());
});

}
}

#include "tests.tcc"
