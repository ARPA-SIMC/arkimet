#include "arki/tests/tests.h"
#include "arki/scan/dir.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_dir");

void Tests::register_tests() {

// Test DirScanner on an empty directory
add_method("empty", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);

    set<string> ds = scan::dir("dirscanner");
    ensure(ds.empty());
});

// Test DirScanner on a populated directory
add_method("dir1", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    utils::createFlagfile("dirscanner/index.sqlite");
    mkdir("dirscanner/2007", 0777);
    mkdir("dirscanner/2008", 0777);
    utils::createFlagfile("dirscanner/2008/a.grib");
    utils::createFlagfile("dirscanner/2008/b.grib");
    mkdir("dirscanner/2008/temp", 0777);
    mkdir("dirscanner/2009", 0777);
    utils::createFlagfile("dirscanner/2009/a.grib");
    utils::createFlagfile("dirscanner/2009/b.grib");
    mkdir("dirscanner/2009/temp", 0777);
    mkdir("dirscanner/.archive", 0777);
    utils::createFlagfile("dirscanner/.archive/z.grib");

    set<string> ds = scan::dir("dirscanner");
    wassert(actual(ds.size()) == 4u);
    auto i = ds.begin();
    wassert(actual(*i++) == "2008/a.grib");
    wassert(actual(*i++) == "2008/b.grib");
    wassert(actual(*i++) == "2009/a.grib");
    wassert(actual(*i++) == "2009/b.grib");
});

// Test file names interspersed with directory names
add_method("dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    utils::createFlagfile("dirscanner/index.sqlite");
    mkdir("dirscanner/2008", 0777);
    utils::createFlagfile("dirscanner/2008/a.grib");
    utils::createFlagfile("dirscanner/2008/b.grib");
    mkdir("dirscanner/2008/a", 0777);
    utils::createFlagfile("dirscanner/2008/a/a.grib");
    mkdir("dirscanner/2009", 0777);
    utils::createFlagfile("dirscanner/2009/a.grib");
    utils::createFlagfile("dirscanner/2009/b.grib");

    set<string> ds = scan::dir("dirscanner");
    wassert(actual(ds.size()) == 5u);
    auto i = ds.begin();
    wassert(actual(*i++) == "2008/a.grib");
    wassert(actual(*i++) == "2008/a/a.grib");
    wassert(actual(*i++) == "2008/b.grib");
    wassert(actual(*i++) == "2009/a.grib");
    wassert(actual(*i++) == "2009/b.grib");
});

}

}
