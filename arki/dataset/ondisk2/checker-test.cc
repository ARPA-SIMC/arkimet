#include "test-utils.h"
#include "writer.h"
#include "reader.h"
#include "arki/dataset/reporter.h"
#include "arki/types/source/blob.h"

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            unique = origin, reftime
        )");
    }

    Metadata& find_imported_second_in_file()
    {
        // Find the imported_result element whose offset is > 0
        for (int i = 0; i < 3; ++i)
            if (import_results[i].sourceBlob().offset > 0)
                return import_results[i];
        throw std::runtime_error("second in file not found");
    }

    void import_and_make_hole(const testdata::Fixture& fixture, std::string& holed_fname)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        wassert(import_all(fixture));
        Metadata& md = find_imported_second_in_file();
        const source::Blob& second_in_segment = md.sourceBlob();
        holed_fname = second_in_segment.filename;

        {
            // Remove one element
            auto writer = makeOndisk2Writer();
            writer->remove(md);
            writer->flush();
        }

        {
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_DIRTY] = 1;
            wassert(actual(*makeOndisk2Checker()).maintenance(expected));
        }
    }

    /// Return the relpath of the segment that has been deleted
    std::string import_and_delete_one_file(const testdata::Fixture& fixture)
    {
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        wassert(import_all(fixture));

        string res = destfile(fixture.test_data[0]);

        // Remove all the elements in one file
        {
            auto writer = makeOndisk2Writer();
            for (int i = 0; i < 3; ++i)
                if (destfile(fixture.test_data[i]) == destfile(fixture.test_data[0]))
                    writer->remove(import_results[i]);
            writer->flush();
        }

        {
            arki::tests::MaintenanceResults expected(false, count_dataset_files(fixture));
            expected.by_type[COUNTED_OK] = count_dataset_files(fixture) - 1;
            expected.by_type[COUNTED_DELETED] = 1;
            wassert(actual(*makeOndisk2Checker()).maintenance(expected));
        }

        return res;
    }
};


template<typename TestData>
class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests<testdata::GRIBData> test_grib("arki_dataset_ondisk2_checker_grib");
Tests<testdata::GRIBData> test_grib_dir("arki_dataset_ondisk2_checker_grib_dir", "segments=dir");
Tests<testdata::BUFRData> test_bufr("arki_dataset_ondisk2_checker_bufr");
Tests<testdata::BUFRData> test_bufr_dir("arki_dataset_ondisk2_checker_bufr_dir", "segments=dir");
Tests<testdata::VM2Data> test_vm2("arki_dataset_ondisk2_checker_vm2");
Tests<testdata::VM2Data> test_vm2_dir("arki_dataset_ondisk2_checker_vm2_dir", "segments=dir");
Tests<testdata::ODIMData> test_odim("arki_dataset_ondisk2_checker_odim");
Tests<testdata::ODIMData> test_odim_dir("arki_dataset_ondisk2_checker_odim_dir", "segments=dir");

template<typename TestData>
void Tests<TestData>::register_tests() {

// Test maintenance scan, on dataset with one file to pack, performing repack
add_method("hole_file_and_repack", [](Fixture& f) {
    TestData fixture;
    string holed_fname;
    wassert(f.import_and_make_hole(fixture, holed_fname));

    {
        // Test packing has something to report
        ReporterExpected e;
        e.repacked.emplace_back("testds", holed_fname);
        wassert(actual(*f.makeOndisk2Checker()).repack(e, false));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        e.repacked.emplace_back("testds", holed_fname);
        wassert(actual(*checker).repack(e, true));

        wassert(actual(*checker).maintenance_clean(2));
    }

    // Ensure that we have the summary cache
    wassert(actual_file("testds/.summaries/all.summary").exists());
    //ensure(sys::fs::exists("testds/.summaries/2007-07.summary"));
    //ensure(sys::fs::exists("testds/.summaries/2007-10.summary"));
});

// Test maintenance scan, on dataset with one file to delete, performing repack
add_method("delete_file_and_repack", [](Fixture& f) {
    TestData fixture;
    string removed_fname = wcallchecked(f.import_and_delete_one_file(fixture));

    {
        // Test packing has something to report
        ReporterExpected e;
        e.deleted.emplace_back("testds", removed_fname);
        wassert(actual(*f.makeOndisk2Checker()).repack(e, false));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        e.deleted.emplace_back("testds", removed_fname);
        wassert(actual(*checker).repack(e, true));

        wassert(actual(*checker).maintenance_clean(f.count_dataset_files(fixture) - 1));
    }

    // Ensure that we have the summary cache
    wassert(actual_file("testds/.summaries/all.summary").exists());
    //ensure(sys::fs::exists("testds/.summaries/2007-07.summary"));
    //ensure(sys::fs::exists("testds/.summaries/2007-10.summary"));
});

// Test maintenance scan, on dataset with one file to pack, performing check
add_method("hole_file_and_check", [](Fixture& f) {
    TestData fixture;
    string holed_fname;
    wassert(f.import_and_make_hole(fixture, holed_fname));

    {
        // Test check has something to report
        ReporterExpected e;
        e.repacked.emplace_back("testds", holed_fname);
        wassert(actual(*f.makeOndisk2Checker()).check(e, false));
    }

    // Check refuses to potentially lose data, so it does nothing in this case
    wassert(actual(*f.makeOndisk2Checker()).check_clean(true));

    // In the end, we are stil left with one file to pack
    {
        arki::tests::MaintenanceResults expected(false, 2);
        expected.by_type[DatasetTest::COUNTED_OK] = 1;
        expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
        wassert(actual(*f.makeOndisk2Checker()).maintenance(expected));
    }

    // Ensure that we have the summary cache
    wassert(actual_file("testds/.summaries/all.summary").exists());
    //ensure(sys::fs::exists("testds/.summaries/2007-07.summary"));
    //ensure(sys::fs::exists("testds/.summaries/2007-10.summary"));
});

// Test maintenance scan, on dataset with one file to delete, performing check
add_method("delete_file_and_check", [](Fixture& f) {
    TestData fixture;
    string deleted_relpath = wcallchecked(f.import_and_delete_one_file(fixture));

    auto state = f.scan_state();
    wassert(actual(state.get(deleted_relpath).state) == segment::State(SEGMENT_DELETED));

    // Perform check, nothing happens
    {
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        wassert(actual(*checker).check(e, true));
    }

    state = f.scan_state();
    wassert(actual(state.get(deleted_relpath).state) == segment::State(SEGMENT_DELETED));

    // Ensure that we have the summary cache
    wassert(actual_file("testds/.summaries/all.summary").exists());
    //ensure(sys::fs::exists("testds/.summaries/2007-07.summary"));
    //ensure(sys::fs::exists("testds/.summaries/2007-10.summary"));
});

// Test accuracy of maintenance scan, after deleting the index
add_method("delete_index_and_check", [](Fixture& f) {
    f.cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
    TestData fixture;
    wassert(f.import_all_packed(fixture));

    // Query everything when the dataset is in a clean state
    metadata::Collection mdc_pre = f.query(Matcher());
    wassert(actual(mdc_pre.size()) == 3u);

    sys::unlink_ifexists("testds/index.sqlite");

    // All files are found as files to be indexed
    {
        arki::tests::MaintenanceResults expected(false, f.count_dataset_files(fixture));
        expected.by_type[DatasetTest::COUNTED_UNALIGNED] = f.count_dataset_files(fixture);
        wassert(actual(*f.makeOndisk2Checker()).maintenance(expected));
    }

    // A check rebuilds the index
    {
        auto checker = f.makeOndisk2Checker();
        ReporterExpected e;
        for (const auto& i: f.destfiles(fixture))
            e.rescanned.emplace_back("testds", i);
        wassert(actual(*checker).check(e, true));

        wassert(actual(*checker).maintenance_clean(f.count_dataset_files(fixture)));
        wassert(actual(*checker).repack_clean(true));
    }

    // Query everything after the rebuild and check that everything is
    // still there
    metadata::Collection mdc_post = f.query(Matcher());
    wassert(actual(mdc_post.size()) == 3u);

    wassert(actual(mdc_post[0]).is_similar(mdc_pre[0]));
    wassert(actual(mdc_post[1]).is_similar(mdc_pre[1]));
    wassert(actual(mdc_post[2]).is_similar(mdc_pre[2]));

    // Ensure that we have the summary cache
    wassert(actual_file("testds/.summaries/all.summary").exists());
});

}

}
