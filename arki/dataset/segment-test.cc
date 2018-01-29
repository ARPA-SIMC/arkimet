#include "arki/dataset/tests.h"
#include "arki/dataset/reporter.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/scan/any.h"
#include "segment.h"
#include "segmented.h"
#include "segment/lines.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class TestItem
{
public:
    std::string relname;

    TestItem(const std::string& relname) : relname(relname) {}
};

struct TestSegments
{
    ConfigFile cfg;
    std::string pathname;
    std::string abspath;
    //const testdata::Fixture& td;

    /**
     * cfg_segments can be:
     *  - dir: directory segments
     *  - (nothing): AutoManager
     */
    TestSegments(const std::string& cfg_segments=std::string())
         : pathname("testsegment.grib"), abspath("testds/testsegment.grib")
    {
        if (sys::isdir("testds"))
            sys::rmtree("testds");
        else
            sys::unlink_ifexists("testds");
        sys::mkdir_ifmissing("testds");

        cfg.setValue("name", "test");
        cfg.setValue("step", "daily");
        cfg.setValue("path", "testds");
        if (!cfg_segments.empty())
            cfg.setValue("segments", cfg_segments);
    }

    void test_repack()
    {
        ConfigFile cfg1(cfg);
        cfg1.setValue("mockdata", "true");
        auto config = dataset::segmented::Config::create(cfg1);
        auto segment_manager = config->create_segment_manager();

        // Import 2 gigabytes of data in a single segment
        metadata::Collection mds;
        {
            auto w = segment_manager->get_writer("test.grib");
            for (unsigned i = 0; i < 2048; ++i)
            {
                unique_ptr<Metadata> md(new Metadata(testdata::make_large_mock("grib", 1024*1024, i / (30 * 24), (i/24) % 30, i % 24)));
                unique_ptr<types::Source> old_source(md->source().clone());
                w->append(*md);
                w->commit();
                md->drop_cached_data();
                mds.acquire(move(md));
            }
        }

        // Repack it
        wassert(segment_manager->repack("test.grib", mds));
    }

    void test_check_empty()
    {
        auto config = dataset::segmented::Config::create(cfg);
        auto segment_manager = config->create_segment_manager();
        dataset::NullReporter reporter;
        metadata::Collection mds;
        auto state = segment_manager->check(reporter, "test", pathname, mds);
        wassert(actual_file(abspath).not_exists());
        wassert(actual(state) == SEGMENT_MISSING);
        wassert(actual(mds.size()) == 0u);
    }

    void test_check_empty_when_compressed_exists()
    {
        auto config = dataset::segmented::Config::create(cfg);
        auto segment_manager = config->create_segment_manager();
        sys::write_file(abspath, "");
        scan::compress(abspath, std::make_shared<core::lock::Null>());
        sys::unlink(abspath);
        dataset::NullReporter reporter;
        metadata::Collection mds;
        auto state = segment_manager->check(reporter, "test", pathname, mds);
        wassert(actual_file(abspath).not_exists());
        wassert(actual(state) == SEGMENT_OK);
        wassert(actual(mds.size()) == 0u);
    }
};

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment");

void Tests::register_tests() {

add_method("file_repack", [] {
    TestSegments ts;
    wassert(ts.test_repack());
});

add_method("file_check_empty", [] {
    TestSegments ts;
    wassert(ts.test_check_empty());
    wassert(ts.test_check_empty_when_compressed_exists());
});

add_method("dir_repack", [] {
    TestSegments ts("dir");
    wassert(ts.test_repack());
});

add_method("dir_check_empty", [] {
    TestSegments ts("dir");
    wassert(ts.test_check_empty());
});


// Test scan_dir on an empty directory
add_method("scan_dir_empty", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);

    {
        auto sm = segment::Manager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }

    {
        auto sm = segment::Manager::get("dirscanner", true);
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
    mkdir("dirscanner/2008/temp", 0777);
    mkdir("dirscanner/2009", 0777);
    sys::write_file("dirscanner/2009/a.grib", "");
    sys::write_file("dirscanner/2009/b.grib", "");
    mkdir("dirscanner/2009/temp", 0777);
    mkdir("dirscanner/.archive", 0777);
    sys::write_file("dirscanner/.archive/z.grib", "");

    {
        auto sm = segment::Manager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 4u);
        std::sort(res.begin(), res.end());

        wassert(actual(res[0]) == "2008/a.grib");
        wassert(actual(res[1]) == "2008/b.grib");
        wassert(actual(res[2]) == "2009/a.grib");
        wassert(actual(res[3]) == "2009/b.grib");
    }

    {
        auto sm = segment::Manager::get("dirscanner", true);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
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
        auto sm = segment::Manager::get("dirscanner", false);
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

    {
        auto sm = segment::Manager::get("dirscanner", true);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});

// odimh5 files are not considered segments
add_method("scan_dir_dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/01.odimh5", "");

    {
        auto sm = segment::Manager::get("dirscanner", false);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }

    {
        auto sm = segment::Manager::get("dirscanner", true);
        std::vector<std::string> res;
        sm->scan_dir([&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});


}

}
