#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/simple.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"

using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

namespace {

using namespace arki::dataset::maintenance_test;

template<typename TestFixture>
class Tests : public MaintenanceTest<TestFixture>
{
    using MaintenanceTest<TestFixture>::MaintenanceTest;
    typedef TestFixture Fixture;

    void register_tests() override;

    bool can_detect_overlap() const override { return true; }
    bool can_detect_segments_out_of_step() const override { return true; }
    bool can_delete_data() const override { return false; }
};

template<typename Fixture>
void Tests<Fixture>::register_tests()
{
    MaintenanceTest<Fixture>::register_tests();

    this->add_method("check_empty_metadata", R"(
    - `.metadata` file must not be empty [unaligned]
    )", [](Fixture& f) {
        sys::File mdf("testds/" + f.test_relpath + ".metadata", O_RDWR);
        mdf.ftruncate(0);
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_metadata_timestamp", R"(
    - `.metadata` file must not be older than the data [unaligned]
    )", [&](Fixture& f) {
        sys::touch("testds/" + f.test_relpath + ".metadata", 1496167200);
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_summary_timestamp", R"(
    - `.summary` file must not be older than the `.metadata` file [unaligned]
    )", [&](Fixture& f) {
        sys::touch("testds/" + f.test_relpath + ".summary", 1496167200);
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_manifest_timestamp", R"(
    - `MANIFEST` file must not be older than the `.metadata` file [unaligned]
    )", [&](Fixture& f) {
        time_t manifest_ts;
        if (sys::exists("testds/MANIFEST"))
            manifest_ts = sys::timestamp("testds/MANIFEST");
        else
            manifest_ts = sys::timestamp("testds/index.sqlite");

        sys::touch("testds/" + f.test_relpath + ".metadata", manifest_ts + 1);

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_missing_index", R"(
        - if the index has been deleted, accessing the dataset recreates it
          empty, and a check will rebuild it. Until it gets rebuilt, segments
          not present in the index would not be considered when querying the
          dataset
    )", [&](Fixture& f) {
        wassert(f.query_results({1, 3, 0, 2}));

        sys::unlink_ifexists("testds/index.sqlite");
        sys::unlink_ifexists("testds/MANIFEST");

        wassert(f.query_results({}));

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_missing_index_spurious_files", R"(
    )", [&](Fixture& f) {
        wassert(f.query_results({1, 3, 0, 2}));

        sys::unlink_ifexists("testds/index.sqlite");
        sys::unlink_ifexists("testds/MANIFEST");
        sys::write_file("testds/2007/11-11." + f.format + ".tmp", f.format + " GARBAGE 7777");

        wassert(f.query_results({}));

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    this->add_method("check_metadata_must_contain_reftimes", R"(
    - metadata in the `.metadata` file must contain reference time elements [corrupted]
    )", [&](Fixture& f) {
        metadata::Collection mds;
        mds.read_from_file("testds/" + f.test_relpath + ".metadata");
        wassert(actual(mds.size()) == 2u);
        for (auto& md: mds)
            md->unset(TYPE_REFTIME);
        mds.writeAtomically("testds/" + f.test_relpath + ".metadata");

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
    });

    this->add_method("repack_unaligned", R"(
        - [unaligned] segments are not touched, to prevent deleting data that
          should be reindexed instead
    )", [&](Fixture& f) {
        f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);

        {
            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            wassert(actual(writer.get()).repack(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });
}

Tests<FixtureConcat> test_simple_grib_plain("arki_dataset_simple_maintenance_grib_plain", "grib", "type=simple\nindex_type=plain\n");
Tests<FixtureConcat> test_simple_grib_sqlite("arki_dataset_simple_maintenance_grib_sqlite", "grib", "type=simple\nindex_type=sqlite");
Tests<FixtureDir> test_simple_grib_plain_dir("arki_dataset_simple_maintenance_grib_plain_dirs", "grib", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureDir> test_simple_grib_sqlite_dir("arki_dataset_simple_maintenance_grib_sqlite_dirs", "grib", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureZip> test_simple_grib_plain_zip("arki_dataset_simple_maintenance_grib_plain_zip", "grib", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureZip> test_simple_grib_sqlite_zip("arki_dataset_simple_maintenance_grib_sqlite_zip", "grib", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureConcat> test_simple_bufr_plain("arki_dataset_simple_maintenance_bufr_plain", "bufr", "type=simple\nindex_type=plain\n");
Tests<FixtureConcat> test_simple_bufr_sqlite("arki_dataset_simple_maintenance_bufr_sqlite", "bufr", "type=simple\nindex_type=sqlite");
Tests<FixtureDir> test_simple_bufr_plain_dir("arki_dataset_simple_maintenance_bufr_plain_dirs", "bufr", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureDir> test_simple_bufr_sqlite_dir("arki_dataset_simple_maintenance_bufr_sqlite_dirs", "bufr", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureZip> test_simple_bufr_plain_zip("arki_dataset_simple_maintenance_bufr_plain_zip", "bufr", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureZip> test_simple_bufr_sqlite_zip("arki_dataset_simple_maintenance_bufr_sqlite_zip", "bufr", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureConcat> test_simple_vm2_plain("arki_dataset_simple_maintenance_vm2_plain", "vm2", "type=simple\nindex_type=plain\n");
Tests<FixtureConcat> test_simple_vm2_sqlite("arki_dataset_simple_maintenance_vm2_sqlite", "vm2", "type=simple\nindex_type=sqlite");
Tests<FixtureDir> test_simple_vm2_plain_dir("arki_dataset_simple_maintenance_vm2_plain_dirs", "vm2", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureDir> test_simple_vm2_sqlite_dir("arki_dataset_simple_maintenance_vm2_sqlite_dirs", "vm2", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureZip> test_simple_vm2_plain_zip("arki_dataset_simple_maintenance_vm2_plain_zip", "vm2", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests<FixtureZip> test_simple_vm2_sqlite_zip("arki_dataset_simple_maintenance_vm2_sqlite_zip", "vm2", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests<FixtureDir> test_simple_odimh5_plain_dir("arki_dataset_simple_maintenance_odimh5_plain", "odimh5", "type=simple\nindex_type=plain\n");
Tests<FixtureDir> test_simple_odimh5_sqlite_dir("arki_dataset_simple_maintenance_odimh5_sqlite", "odimh5", "type=simple\nindex_type=sqlite\n");
Tests<FixtureZip> test_simple_odimh5_plain_zip("arki_dataset_simple_maintenance_odimh5_plain_zip", "odimh5", "type=simple\nindex_type=plain\n");
Tests<FixtureZip> test_simple_odimh5_sqlite_zip("arki_dataset_simple_maintenance_odimh5_sqlite_zip", "odimh5", "type=simple\nindex_type=sqlite\n");

}
