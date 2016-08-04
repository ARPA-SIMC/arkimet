#include "arki/dataset/tests.h"
#include "arki/dataset.h"
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
    std::set<std::string> destfiles_before_cutoff;
    std::set<std::string> destfiles_after_cutoff;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            unique=reftime, origin, product, level, timerange, area
            step=daily
        )");

        destfiles_before_cutoff.clear();
        destfiles_after_cutoff.clear();
        // Partition data in two groups: before and after selective_cutoff
        for (unsigned i = 0; i < 3; ++i)
            if (td.test_data[i].time < td.selective_cutoff)
                destfiles_before_cutoff.insert(destfile(td.test_data[i]));
            else
                destfiles_after_cutoff.insert(destfile(td.test_data[i]));
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
TestsChecker<testdata::BUFRData> test_checker_bufr_ondisk2("arki_dataset_checker_bufr_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_ondisk2_sharded("arki_dataset_checker_bufr_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_plain("arki_dataset_checker_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_plain_sharded("arki_dataset_checker_bufr_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::BUFRData> test_checker_bufr_simple_sqlite("arki_dataset_checker_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::VM2Data> test_checker_vm2_ondisk2("arki_dataset_checker_vm2_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_ondisk2_sharded("arki_dataset_checker_vm2_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_plain("arki_dataset_checker_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_plain_sharded("arki_dataset_checker_vm2_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::VM2Data> test_checker_vm2_simple_sqlite("arki_dataset_checker_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsChecker<testdata::ODIMData> test_checker_odim_ondisk2("arki_dataset_checker_odim_ondisk2", "type=ondisk2\n");
TestsChecker<testdata::ODIMData> test_checker_odim_ondisk2_sharded("arki_dataset_checker_odim_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_plain("arki_dataset_checker_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_plain_sharded("arki_dataset_checker_odim_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsChecker<testdata::ODIMData> test_checker_odim_simple_sqlite("arki_dataset_checker_odim_simple_sqlite", "type=simple\nindex_type=sqlite");

template<class Data>
void TestsChecker<Data>::register_tests() {

typedef FixtureChecker<Data> Fixture;

this->add_method("preconditions", [](Fixture& f) {
    wassert(actual(f.destfiles_before_cutoff.size()) > 0u);
    wassert(actual(f.destfiles_after_cutoff.size()) > 0u);
    wassert(actual(f.destfiles_before_cutoff.size() + f.destfiles_after_cutoff.size()) == f.count_dataset_files(f.td));
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
    f.cfg.setValue("archive age", f.td.selective_days_since());
    wassert(f.import_all(f.td));

    // Check if files to archive are detected
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        //e.report.emplace_back("testds.archives.last", "check", nfiles(f.td.fnames_before_cutoff.size()) + " ok");
        e.report.emplace_back("testds", "repack", nfiles(f.destfiles_after_cutoff.size()) + " ok");
        for (const auto& fn: f.destfiles_before_cutoff)
            e.archived.emplace_back("testds", fn);
        wassert(actual(checker.get()).repack(e, false));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        for (const auto& i: f.destfiles_before_cutoff)
            e.archived.emplace_back("testds", i);
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    for (const auto& i: f.destfiles_before_cutoff)
    {
        wassert(actual_file("testds/.archive/last/" + i).exists());
        wassert(actual_file("testds/.archive/last/" + i + ".metadata").exists());
        wassert(actual_file("testds/.archive/last/" + i + ".summary").exists());
        wassert(actual_file("testds/" + i).not_exists());
    }

    // Maintenance should now show a normal situation
    {
        auto checker(f.config().create_checker());
        ReporterExpected e;
        e.report.emplace_back("testds.archives.last", "check", nfiles(f.destfiles_before_cutoff.size()) + " ok");
        e.report.emplace_back("testds", "check", nfiles(f.destfiles_after_cutoff.size()) + " ok");
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
    f.cfg.setValue("delete age", f.td.selective_days_since());
    wassert(f.import_all(f.td));

    {
        auto checker(f.config().create_checker());
        ReporterExpected e;

        // Check if files to delete are detected
        e.clear();
        for (const auto& i: f.destfiles_before_cutoff)
            e.deleted.emplace_back("testds", i);
        wassert(actual(checker.get()).repack(e, false));

        // Perform packing and check that things are still ok afterwards
        e.clear();
        for (const auto& i: f.destfiles_before_cutoff)
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
     * Here we have inconsistent behaviou across segment types and data types,
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

}
}
