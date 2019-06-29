#include "arki/dataset/tests.h"
#include "arki/dataset/reporter.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "segment.h"
#include "segmented.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment");

void Tests::register_tests() {

// Test scan_dir on an empty directory
add_method("scan_dir_empty", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);

    {
        auto sm = SegmentManager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }

    {
        auto sm = SegmentManager::get("dirscanner", true);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});

// Test scan_dir on a populated directory
add_method("scan_dir_dir1", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    sys::write_file("dirscanner/index.sqlite", "");
    mkdir("dirscanner/2007", 0777);
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/a.grib", "");
    sys::write_file("dirscanner/2008/b.grib", "");
    sys::write_file("dirscanner/2008/c.grib.gz", "");
    sys::write_file("dirscanner/2008/d.grib.tar", "");
    sys::write_file("dirscanner/2008/e.grib.zip", "");
    mkdir("dirscanner/2008/temp", 0777);
    mkdir("dirscanner/2009", 0777);
    sys::write_file("dirscanner/2009/a.grib", "");
    sys::write_file("dirscanner/2009/b.grib", "");
    mkdir("dirscanner/2009/temp", 0777);
    mkdir("dirscanner/.archive", 0777);
    sys::write_file("dirscanner/.archive/z.grib", "");

    {
        auto sm = SegmentManager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 7u);
        std::sort(res.begin(), res.end());

        wassert(actual(res[0]) == "2008/a.grib");
        wassert(actual(res[1]) == "2008/b.grib");
        wassert(actual(res[2]) == "2008/c.grib");
        wassert(actual(res[3]) == "2008/d.grib");
        wassert(actual(res[4]) == "2008/e.grib");
        wassert(actual(res[5]) == "2009/a.grib");
        wassert(actual(res[6]) == "2009/b.grib");
    }
});

// Test file names interspersed with directory names
add_method("scan_dir_dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    sys::write_file("dirscanner/index.sqlite", "");
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/a.grib", "");
    sys::write_file("dirscanner/2008/b.grib", "");
    mkdir("dirscanner/2008/a", 0777);
    sys::write_file("dirscanner/2008/a/a.grib", "");
    mkdir("dirscanner/2009", 0777);
    sys::write_file("dirscanner/2009/a.grib", "");
    sys::write_file("dirscanner/2009/b.grib", "");

    {
        auto sm = SegmentManager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 5u);
        std::sort(res.begin(), res.end());

        wassert(actual(res[0]) == "2008/a.grib");
        wassert(actual(res[1]) == "2008/a/a.grib");
        wassert(actual(res[2]) == "2008/b.grib");
        wassert(actual(res[3]) == "2009/a.grib");
        wassert(actual(res[4]) == "2009/b.grib");
    }
});

// odimh5 files are not considered segments
add_method("scan_dir_dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/01.odimh5", "");

    {
        auto sm = SegmentManager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }

    {
        auto sm = SegmentManager::get("dirscanner", true);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});


}

}
