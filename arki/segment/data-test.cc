#include "arki/core/tests.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "segment/data.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

void make_file(const std::filesystem::path& name)
{
    delete_if_exists(name);
    sys::write_file(name, "");
}

void make_dir(const std::filesystem::path& name)
{
    delete_if_exists(name);
    std::filesystem::create_directory(name);
    sys::write_file(name / ".sequence", "");
}

void make_samples()
{
    // FIXME: move these into a subdir, which makes setup/cleanup operations easier
    make_file("testfile.grib");
    make_file("testfile.bufr");
    make_file("testfile.vm2");
    make_file("testfile.h5");
    make_dir("testdir.grib");
    make_dir("testdir.bufr");
    make_dir("testdir.vm2");
    make_dir("testdir.h5");
    make_file("testtar.grib.tar");
    make_file("testtar.bufr.tar");
    make_file("testtar.vm2.tar");
    make_file("testtar.h5.tar");
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_data");

void Tests::register_tests() {

add_method("auto_instantiate_existing", [] {
    make_samples();

    auto get_writer = [&](const char* format, const char* name) {
        segment::data::WriterConfig writer_config;
        auto segment = std::make_shared<Segment>(format_from_string(format), ".", name);
        return segment->detect_data_writer(writer_config);
    };
    auto get_checker = [&](const char* format, const char* name) {
        auto segment = std::make_shared<Segment>(format_from_string(format), ".", name);
        return segment->detect_data_checker();
    };

    wassert(actual(get_writer("grib", "testfile.grib")->data().type()) == "concat");
    wassert(actual(get_writer("bufr", "testfile.bufr")->data().type()) == "concat");
    wassert(actual(get_writer("vm2", "testfile.vm2")->data().type()) == "lines");
    wassert_throws(std::runtime_error, get_writer("odimh5", "testfile.h5"));
    wassert(actual(get_writer("grib", "testdir.grib")->data().type()) == "dir");
    wassert(actual(get_writer("bufr", "testdir.bufr")->data().type()) == "dir");
    wassert(actual(get_writer("vm2", "testdir.vm2")->data().type()) == "dir");
    wassert(actual(get_writer("odimh5", "testdir.h5")->data().type()) == "dir");
    wassert_throws(std::runtime_error, get_writer("grib", "testtar.grib"));
    wassert_throws(std::runtime_error, get_writer("bufr", "testtar.bufr"));
    wassert_throws(std::runtime_error, get_writer("vm2", "testtar.vm2"));
    wassert_throws(std::runtime_error, get_writer("odimh5", "testtar.h5"));

    wassert(actual(get_checker("grib", "testfile.grib")->data().type()) == "concat");
    wassert(actual(get_checker("bufr", "testfile.bufr")->data().type()) == "concat");
    wassert(actual(get_checker("vm2", "testfile.vm2")->data().type()) == "lines");
    // wassert(actual(get_checker("odimh5", "testfile.h5")->data().type()) == "dir");
    wassert_throws(std::runtime_error, get_checker("odimh5", "testfile.h5"));
    wassert(actual(get_checker("grib", "testdir.grib")->data().type()) == "dir");
    wassert(actual(get_checker("bufr", "testdir.bufr")->data().type()) == "dir");
    wassert(actual(get_checker("vm2", "testdir.vm2")->data().type()) == "dir");
    wassert(actual(get_checker("odimh5", "testdir.h5")->data().type()) == "dir");
    wassert(actual(get_checker("grib", "testtar.grib")->data().type()) == "tar");
    wassert(actual(get_checker("bufr", "testtar.bufr")->data().type()) == "tar");
    wassert(actual(get_checker("vm2", "testtar.vm2")->data().type()) == "tar");
    wassert(actual(get_checker("odimh5", "testtar.h5")->data().type()) == "tar");
});

}

}
