#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template<class Data>
struct FixtureChecker : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;
    std::set<std::string> relpaths_old;
    std::set<std::string> relpaths_new;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            unique=reftime, origin, product, level, timerange, area
            step=daily
        )");

        // Partition data in two groups: before and after selective_cutoff
        relpaths_old.clear();
        relpaths_new.clear();
        for (const auto& el: td.test_data)
            if (el.time >= td.selective_cutoff)
                relpaths_new.insert(destfile(el));
            else
                relpaths_old.insert(destfile(el));
    }

    const types::source::Blob& find_imported_second_in_file()
    {
        // Find the imported_result element whose offset is > 0
        for (int i = 0; i < 3; ++i)
        {
            const types::source::Blob& second_in_segment = import_results[i].sourceBlob();
            if (second_in_segment.offset > 0)
                return second_in_segment;
        }
        throw std::runtime_error("second in file not found");
    }
};


template<class Data>
class TestsChecker : public FixtureTestCase<FixtureChecker<Data>>
{
    using FixtureTestCase<FixtureChecker<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsChecker<testdata::GRIBData> test_checker_grib_ondisk2("arki_dataset_checker_grib_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::GRIBData> test_checker_grib_simple_plain("arki_dataset_checker_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::GRIBData> test_checker_grib_simple_sqlite("arki_dataset_checker_grib_simple_sqlite", "type=simple\nindex_type=sqlite\n");
TestsChecker<testdata::GRIBData> test_checker_grib_iseg("arki_dataset_checker_grib_iseg", "type=iseg\nformat=grib\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_ondisk2("arki_dataset_checker_bufr_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_plain("arki_dataset_checker_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_sqlite("arki_dataset_checker_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::BUFRData> test_checker_bufr_iseg("arki_dataset_checker_bufr_iseg", "type=iseg\nformat=bufr\n");

template<class Data>
void TestsChecker<Data>::register_tests() {

typedef FixtureChecker<Data> Fixture;

this->add_method("preconditions", [](Fixture& f) {
    wassert(actual(f.relpaths_old.size()) > 0u);
    wassert(actual(f.relpaths_new.size()) > 0u);
    wassert(actual(f.relpaths_old.size() + f.relpaths_new.size()) == f.count_dataset_files(f.td));
});

// Test check_issue51
this->add_method("check_issue51", [](Fixture& f) {
    f.cfg.setValue("step", "yearly");
    if (f.td.format != "grib" && f.td.format != "bufr") return;
    wassert(f.import_all_packed(f.td));

    // Get metadata for all data in the dataset and corrupt the last character
    // of them all
    metadata::Collection mds = f.query(Matcher());
    wassert(actual(mds.size()) == 3u);
    set<string> destfiles;
    for (const auto& md: mds)
    {
        const auto& blob = md->sourceBlob();
        destfiles.insert(blob.filename);
        File f(blob.absolutePathname(), O_RDWR);
        f.lseek(blob.offset + blob.size - 1);
        f.write_all_or_throw("\x0d", 1);
        f.close();
    }

    auto checker(f.config().create_checker());

    // See if check_issue51 finds the problem
    {
        ReporterExpected e;
        for (const auto& relpath: destfiles)
            e.issue51.emplace_back("testds", relpath, "segment contains data with corrupted terminator signature");
        wassert(actual(checker.get()).check_issue51(e, false));
    }

    // See if check_issue51 fixes the problem
    {
        ReporterExpected e;
        for (const auto& relpath: destfiles)
            e.issue51.emplace_back("testds", relpath, "fixed corrupted terminator signatures");
        wassert(actual(checker.get()).check_issue51(e, true));
    }

    // Check that the backup files exist
    for (const auto& relpath: destfiles)
        wassert(actual_file(str::joinpath(f.local_config()->path, relpath) + ".issue51").exists());

    // Do a thorough check to see if everything is ok
    wassert(actual(checker.get()).check_clean(false, false));
});

}
}
