#include "arki/core/tests.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils.h"
#include "segment.h"
#include "segment/lines.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

void clear(const std::string& name)
{
    if (sys::isdir(name))
        sys::rmtree(name);
    else if (sys::exists(name))
        sys::unlink(name);
}

void make_file(const std::string& name)
{
    clear(name);
    sys::write_file(name, "");
}

void make_dir(const std::string& name)
{
    clear(name);
    sys::mkdir_ifmissing(name);
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

    auto get_writer = [&](const char* name) {
        return segment::Writer::for_pathname(utils::get_format(name), ".", name, sys::abspath(name));
    };
    auto get_checker = [&](const char* name) {
        return segment::Checker::for_pathname(utils::get_format(name), ".", name, sys::abspath(name));
    };

    wassert(actual(get_writer("testfile.grib")->type()) == "concat");
    wassert(actual(get_writer("testfile.bufr")->type()) == "concat");
    wassert(actual(get_writer("testfile.vm2")->type()) == "lines");
    wassert_throws(std::runtime_error, get_writer("testfile.h5"));
    wassert(actual(get_writer("testdir.grib")->type()) == "dir");
    wassert(actual(get_writer("testdir.bufr")->type()) == "dir");
    wassert(actual(get_writer("testdir.vm2")->type()) == "dir");
    wassert(actual(get_writer("testdir.h5")->type()) == "dir");
    wassert_throws(std::runtime_error, get_writer("testtar.grib"));
    wassert_throws(std::runtime_error, get_writer("testtar.bufr"));
    wassert_throws(std::runtime_error, get_writer("testtar.vm2"));
    wassert_throws(std::runtime_error, get_writer("testtar.h5"));

    wassert(actual(get_checker("testfile.grib")->type()) == "concat");
    wassert(actual(get_checker("testfile.bufr")->type()) == "concat");
    wassert(actual(get_checker("testfile.vm2")->type()) == "lines");
    wassert(actual(get_checker("testfile.h5")->type()) == "dir");
    wassert(actual(get_checker("testdir.grib")->type()) == "dir");
    wassert(actual(get_checker("testdir.bufr")->type()) == "dir");
    wassert(actual(get_checker("testdir.vm2")->type()) == "dir");
    wassert(actual(get_checker("testdir.h5")->type()) == "dir");
    wassert(actual(get_checker("testtar.grib")->type()) == "tar");
    wassert(actual(get_checker("testtar.bufr")->type()) == "tar");
    wassert(actual(get_checker("testtar.vm2")->type()) == "tar");
    wassert(actual(get_checker("testtar.h5")->type()) == "tar");
});

}

}
