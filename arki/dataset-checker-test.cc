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
TestsChecker<testdata::GRIBData> test_checker_grib_ondisk2_sharded("arki_dataset_checker_grib_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::GRIBData> test_checker_grib_simple_plain("arki_dataset_checker_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::GRIBData> test_checker_grib_simple_plain_sharded("arki_dataset_checker_grib_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::GRIBData> test_checker_grib_simple_sqlite("arki_dataset_checker_grib_simple_sqlite", "type=simple\nindex_type=sqlite\n");
TestsChecker<testdata::GRIBData> test_checker_grib_iseg("arki_dataset_checker_grib_iseg", "type=iseg\nformat=grib\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_ondisk2("arki_dataset_checker_bufr_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_ondisk2_sharded("arki_dataset_checker_bufr_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_plain("arki_dataset_checker_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_plain_sharded("arki_dataset_checker_bufr_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_sqlite("arki_dataset_checker_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::BUFRData> test_checker_bufr_iseg("arki_dataset_checker_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_ondisk2("arki_dataset_checker_vm2_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_ondisk2_sharded("arki_dataset_checker_vm2_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_plain("arki_dataset_checker_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_plain_sharded("arki_dataset_checker_vm2_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_sqlite("arki_dataset_checker_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::VM2Data> test_checker_vm2_iseg("arki_dataset_checker_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsChecker<testdata::ODIMData> test_checker_odim_ondisk2("arki_dataset_checker_odim_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::ODIMData> test_checker_odim_ondisk2_sharded("arki_dataset_checker_odim_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_plain("arki_dataset_checker_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_plain_sharded("arki_dataset_checker_odim_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_sqlite("arki_dataset_checker_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::ODIMData> test_checker_odim_iseg("arki_dataset_checker_odim_iseg", "type=iseg\nformat=odim\n");

template<class Data>
void TestsChecker<Data>::register_tests() {

typedef FixtureChecker<Data> Fixture;

this->add_method("preconditions", [](Fixture& f) {
    wassert(actual(f.relpaths_old.size()) > 0u);
    wassert(actual(f.relpaths_new.size()) > 0u);
    wassert(actual(f.relpaths_old.size() + f.relpaths_new.size()) == f.count_dataset_files(f.td));
});

// Test accuracy of maintenance scan, on perfect dataset
this->add_method("clean", [](Fixture& f) {
    wassert(f.import_all(f.td));

    // Ensure everything appears clean
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).check_clean(false, true));

        // Check that maintenance does not accidentally create an archive
        wassert(actual_file("testds/.archive").not_exists());
    }

    // Ensure packing has nothing to report
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).repack_clean(false));
        wassert(actual(checker.get()).check_clean(false, true));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).repack_clean(true));
        wassert(actual(checker.get()).check_clean(false, true));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).check_clean(true, true));
        wassert(actual(checker.get()).check_clean(false, true));
    }
});

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
this->add_method("archive_age", [](Fixture& f) {
    wassert(f.import_all(f.td));
    f.test_reread_config();
    f.cfg.setValue("archive age", f.td.selective_days_since());

    // Check if files to archive are detected
    {
        auto checker(f.config().create_checker());

        ReporterExpected e;
        e.report.emplace_back("testds", "repack", nfiles(f.relpaths_new.size()) + " ok");
        for (const auto& df: f.relpaths_old)
            e.archived.emplace_back("testds", df);
        wassert(actual(checker.get()).repack(e, false));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        for (const auto& i: f.relpaths_old)
            e.archived.emplace_back("testds", i);
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    for (const auto& el: f.td.test_data)
    {
        if (el.time >= f.td.selective_cutoff) continue;
        string relpath = f.destfile(el);
        string arcrelpath = f.archive_destfile(el);
        wassert(actual_file("testds/" + arcrelpath).exists());
        wassert(actual_file("testds/" + arcrelpath + ".metadata").exists());
        wassert(actual_file("testds/" + arcrelpath + ".summary").exists());
        wassert(actual_file("testds/" + relpath).not_exists());
    }

    // Maintenance should now show a normal situation
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        e.report.emplace_back("testds.archives.last", "check", nfiles(f.relpaths_old.size()) + " ok");
        e.report.emplace_back("testds", "check", nfiles(f.relpaths_new.size()) + " ok");
        wassert(actual(checker.get()).check(e, false, true));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).check_clean(true, true));
        wassert(actual(checker.get()).repack_clean(false));
    }

    // Test that querying returns all items
    {
        auto reader(f.config().create_reader());

        unsigned count = 0;
        reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
        wassert(actual(count) == 3u);
    }
});

// Test accuracy of maintenance scan, on perfect dataset, with data to delete
this->add_method("delete_age", [](Fixture& f) {
    wassert(f.import_all(f.td));
    f.test_reread_config();
    f.cfg.setValue("delete age", f.td.selective_days_since());

    {
        auto checker(f.config().create_checker());
        ReporterExpected e;

        // Check if files to delete are detected
        e.clear();
        for (const auto& i: f.relpaths_old)
            e.deleted.emplace_back("testds", i);
        wassert(actual(checker.get()).repack(e, false));

        // Perform packing and check that things are still ok afterwards
        e.clear();
        for (const auto& i: f.relpaths_old)
            e.deleted.emplace_back("testds", i);
        wassert(actual(checker.get()).repack(e, true));
    }

    auto checker(f.config().create_checker());
    wassert(actual(checker.get()).check_clean(false, true));
});

// Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
this->add_method("scan_truncated", [](Fixture& f) {
    f.cfg.setValue("step", f.td.max_selective_aggregation);
    wassert(f.import_all(f.td));

    // Find the imported_result element whose offset is > 0
    const auto& second_in_segment = f.find_imported_second_in_file();
    wassert(actual(second_in_segment.offset) > 0u);

    // Truncate at the position in second_in_segment
    string truncated_fname = second_in_segment.filename;
    f.segments().truncate(truncated_fname, second_in_segment.offset);

    // See that the truncated file is detected
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        e.rescanned.emplace_back("testds", truncated_fname);
        wassert(actual(checker.get()).check(e, false, true));
    }

    // Perform packing and check that nothing has happened
    {
        auto checker(f.config().create_checker());
        wassert(actual(checker.get()).repack_clean(true));

        ReporterExpected e;
        e.rescanned.emplace_back("testds", truncated_fname);
        wassert(actual(checker.get()).check(e, false, true));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        e.rescanned.emplace_back("testds", truncated_fname);
        wassert(actual(checker.get()).check(e, true, true));

        wassert(actual(checker.get()).check_clean(false, true));
    }

    // Try querying and make sure we get the two files
    {
        auto reader(f.config().create_reader());

        unsigned count = 0;
        reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
        wassert(actual(count) == 2u);
    }
});

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
this->add_method("scan_corrupted", [](Fixture& f) {
    /**
     * Here we have inconsistent behaviour across segment types and data types,
     * because:
     *  - some formats detect corruption, some formats skip garbage
     *  - concatenated data in files may skip corrupted data as garbage, but
     *  directory segments are already split in separate data units and will
     *  load, instead of skipping, corrupted data
     */
    // TODO: VM2 scanning does not yet deal gracefully with corruption
    // TODO: ODIM scanning does not yet deal gracefully with corruption
    f.cfg.setValue("step", f.td.max_selective_aggregation);
    wassert(f.import_all_packed(f.td));

    // Find the imported_result element whose offset is > 0
    const auto& second_in_segment = f.find_imported_second_in_file();
    wassert(actual(second_in_segment.offset) > 0u);

    // Corrupt the first datum in the file
    corrupt_datafile(second_in_segment.absolutePathname());

    {
        ReporterExpected e;
        auto checker(f.config().create_checker());

        // A quick check has nothing to complain
        wassert(actual(checker.get()).check_clean(false, true));

        // A thorough check should find the corruption
        e.clear();
        e.rescanned.emplace_back("testds", second_in_segment.filename);
        wassert(actual(checker.get()).check(e, false, false));

        // Perform full maintenance and check that things are still ok afterwards
        e.clear();
        e.rescanned.emplace_back("testds", second_in_segment.filename);
        wassert(actual(checker.get()).check(e, true, false));

        // The corrupted file has been deindexed, now there is a gap in the data file
        e.clear();
        e.repacked.emplace_back("testds", second_in_segment.filename);
        wassert(actual(checker.get()).check(e, false, false));

        // Perform packing and check that things are still ok afterwards
        e.clear();
        e.repacked.emplace_back("testds", second_in_segment.filename);
        wassert(actual(checker.get()).repack(e, true));

        // Maintenance and pack are ok now
        wassert(actual(checker.get()).check_clean(false, false));
        wassert(actual(checker.get()).repack_clean(false));
    }

    // Try querying and make sure we get the two files
    auto reader(f.config().create_reader());
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 2u);
});

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing repack
this->add_method("repack_deleted", [](Fixture& f) {
    f.cfg.setValue("step", f.td.max_selective_aggregation);
    wassert(f.import_all_packed(f.td));
    string deleted_fname = f.import_results[f.td.max_selective_aggregation_singleton_index].sourceBlob().filename;
    f.segments().remove(deleted_fname);

    {
        auto checker(f.config().create_checker());
        ReporterExpected e;

        // Initial check finds the deleted file
        e.clear();
        e.deindexed.emplace_back("testds", deleted_fname);
        wassert(actual(checker.get()).check(e, false, true));

        // Packing finds the deleted file
        e.clear();
        e.deindexed.emplace_back("testds", deleted_fname);
        wassert(actual(checker.get()).repack(e, false));

        // Perform packing and check that things are still ok afterwards
        e.clear();
        e.deindexed.emplace_back("testds", deleted_fname);
        wassert(actual(checker.get()).repack(e, true));

        wassert(actual(checker.get()).check_clean(false, true));
        wassert(actual(checker.get()).repack_clean(false));
    }

    // Try querying and make sure we get the two files
    auto reader(f.config().create_reader());
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 2u);
});

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing check
this->add_method("check_deleted", [](Fixture& f) {
    f.cfg.setValue("step", f.td.max_selective_aggregation);
    wassert(f.import_all_packed(f.td));
    string deleted_fname = f.import_results[f.td.max_selective_aggregation_singleton_index].sourceBlob().filename;
    f.segments().remove(deleted_fname);

    {
        auto checker(f.config().create_checker());
        ReporterExpected e;

        // Initial check finds the deleted file
        e.clear();
        e.deindexed.emplace_back("testds", deleted_fname);
        wassert(actual(checker.get()).check(e, false, true));

        // Perform full maintenance and check that things are still ok afterwards
        e.clear();
        e.deindexed.emplace_back("testds", deleted_fname);
        wassert(actual(checker.get()).check(e, true, true));

        wassert(actual(checker.get()).check_clean(false, true));
    }

    // Try querying and make sure we get the two files
    auto reader(f.config().create_reader());
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 2u);
});

// Test accuracy of maintenance scan, after deleting the index, with some
// spurious extra files in the dataset
this->add_method("scan_noindex", [](Fixture& f) {
    wassert(f.import_all_packed(f.td));
    sys::unlink_ifexists("testds/index.sqlite");
    sys::unlink_ifexists("testds/MANIFEST");
    for (unsigned i = 0; i < 3; ++i)
    {
        char buf[32];
        snprintf(buf, 32, "testds/%04d/index.sqlite", f.td.test_data[i].time.ye);
        sys::unlink_ifexists(buf);
        snprintf(buf, 32, "testds/%04d/MANIFEST", f.td.test_data[i].time.ye);
        sys::unlink_ifexists(buf);
    }
    sys::makedirs("testds/2014/");
    sys::write_file("testds/2014/01.grib1.tmp", "GRIB garbage 7777");

    {
        auto checker(f.config().create_checker());
        ReporterExpected e;

        // See if the files to index are detected
        e.clear();
        for (const auto& i: f.td.test_data)
            e.rescanned.emplace_back("testds", f.destfile(i));
        wassert(actual(checker.get()).check(e, false, true));

        // Perform full maintenance and check that things are still ok afterwards
        e.clear();
        for (const auto& i: f.td.test_data)
            e.rescanned.emplace_back("testds", f.destfile(i));
        wassert(actual(checker.get()).check(e, true, true));

        wassert(actual(checker.get()).check_clean(false, true));
        wassert(actual(checker.get()).repack_clean(false));

        // The spurious file should not have been touched
        wassert(actual_file("testds/2014/01.grib1.tmp").exists());
    }

    // Try querying and make sure we get the three files
    auto reader(f.config().create_reader());
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
    wassert(actual(count) == 3u);
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
        if (f.cfg.value("shard").empty())
            destfiles.insert(blob.filename);
        else
            destfiles.insert(str::joinpath(str::basename(blob.basedir), blob.filename));
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
