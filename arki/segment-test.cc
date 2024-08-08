#include "arki/core/tests.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "segment.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

void make_file(const std::string& name)
{
    delete_if_exists(name);
    sys::write_file(name, "");
}

void make_dir(const std::string& name)
{
    delete_if_exists(name);
    std::filesystem::create_directory(name);
    sys::write_file(name + "/.sequence", "");
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
} test("arki_segment");

void Tests::register_tests() {

add_method("auto_instantiate_existing", [] {
    make_samples();

    auto get_writer = [&](const char* format, const char* name) {
        segment::WriterConfig writer_config;
        return Segment::detect_writer(writer_config, format, ".", name, sys::abspath(name));
    };
    auto get_checker = [&](const char* format, const char* name) {
        return Segment::detect_checker(format, ".", name, sys::abspath(name));
    };

    wassert(actual(get_writer("grib", "testfile.grib")->segment().type()) == "concat");
    wassert(actual(get_writer("bufr", "testfile.bufr")->segment().type()) == "concat");
    wassert(actual(get_writer("vm2", "testfile.vm2")->segment().type()) == "lines");
    wassert_throws(std::runtime_error, get_writer("odimh5", "testfile.h5"));
    wassert(actual(get_writer("grib", "testdir.grib")->segment().type()) == "dir");
    wassert(actual(get_writer("bufr", "testdir.bufr")->segment().type()) == "dir");
    wassert(actual(get_writer("vm2", "testdir.vm2")->segment().type()) == "dir");
    wassert(actual(get_writer("odimh5", "testdir.h5")->segment().type()) == "dir");
    wassert_throws(std::runtime_error, get_writer("grib", "testtar.grib"));
    wassert_throws(std::runtime_error, get_writer("bufr", "testtar.bufr"));
    wassert_throws(std::runtime_error, get_writer("vm2", "testtar.vm2"));
    wassert_throws(std::runtime_error, get_writer("odimh5", "testtar.h5"));

    wassert(actual(get_checker("grib", "testfile.grib")->segment().type()) == "concat");
    wassert(actual(get_checker("bufr", "testfile.bufr")->segment().type()) == "concat");
    wassert(actual(get_checker("vm2", "testfile.vm2")->segment().type()) == "lines");
    wassert(actual(get_checker("odimh5", "testfile.h5")->segment().type()) == "dir");
    wassert(actual(get_checker("grib", "testdir.grib")->segment().type()) == "dir");
    wassert(actual(get_checker("bufr", "testdir.bufr")->segment().type()) == "dir");
    wassert(actual(get_checker("vm2", "testdir.vm2")->segment().type()) == "dir");
    wassert(actual(get_checker("odimh5", "testdir.h5")->segment().type()) == "dir");
    wassert(actual(get_checker("grib", "testtar.grib")->segment().type()) == "tar");
    wassert(actual(get_checker("bufr", "testtar.bufr")->segment().type()) == "tar");
    wassert(actual(get_checker("vm2", "testtar.vm2")->segment().type()) == "tar");
    wassert(actual(get_checker("odimh5", "testtar.h5")->segment().type()) == "tar");
});

}

}
