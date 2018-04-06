#include "arki/dataset/tests.h"
#include "arki/utils/sys.h"
#include "arki/utils.h"
#include "managers.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment_managers");

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

void Tests::register_tests() {

add_method("auto_instantiate_new", [] {
    make_samples();
});

add_method("forcedir_instantiate_new", [] {
    make_samples();
});

}
}
