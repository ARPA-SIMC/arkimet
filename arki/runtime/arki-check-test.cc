#include "arki/dataset/tests.h"
#include "arki/segment/tests.h"
#include "arki/dataset/time.h"
#include "arki/runtime/tests.h"
#include "arki/exceptions.h"
#include "arki/types/source/blob.h"
#include "arki-check.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step=daily
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
};

Tests test1("arki_runtime_arkicheck_ondisk2", "type=ondisk2");
Tests test2("arki_runtime_arkicheck_simple_plain", "type=simple\nindex_type=plain");
Tests test3("arki_runtime_arkicheck_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests test4("arki_runtime_arkicheck_iseg", "type=iseg\nformat=grib");

void Tests::register_tests() {

add_method("remove_old", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.set("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.odimh5/00.odimh5");
    f.import("inbound/fixture.odimh5/01.odimh5");
    f.import("inbound/fixture.odimh5/02.odimh5");

    auto o = dataset::SessionTime::local_override(1184104800); // date +%s --date="2007-07-11"
    f.cfg.set("archive age", "2");
    f.test_reread_config();
    f.repack();

    f.cfg.set("delete age", "1");
    f.test_reread_config();

    wassert(actual_file("testds/.archive/last/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove-old", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds:2007/07-08.odimh5: segment old enough to be deleted",
            "testds:2007/07-08.odimh5: should be deleted",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove-old", "--offline", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            // "testds.archives.last:2007/07-07.odimh5: segment old enough to be deleted\n"
            "testds.archives.last:2007/07-07.odimh5: should be deleted"
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove-old", "--online", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds:2007/07-08.odimh5: segment old enough to be deleted",
            "testds:2007/07-08.odimh5: should be deleted",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove-old", "--offline", "--online", "testds" });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds:2007/07-08.odimh5: segment old enough to be deleted",
            "testds:2007/07-08.odimh5: should be deleted",
            "testds.archives.last:2007/07-07.odimh5: should be deleted",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove-old", "testds", "-f" });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_match({
            "testds:2007/07-08.odimh5: segment old enough to be deleted",
            "testds:2007/07-08.odimh5: deleted \\([0-9]+ freed\\)",
        }));
        wassert(actual(res) == 0);
    }

    wassert(f.archived_segment_exists("last/2007/07-07.odimh5", {""}));
    wassert(actual_segment("testds/2007/07-08.odimh5").not_exists());
    wassert(f.online_segment_exists("2007/10-09.odimh5", {""}));

    wassert(f.ensure_localds_clean(2, 2));
    wassert(f.query_results({1, 2}));
});

add_method("remove", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    f.skip_if_type_simple();

    f.cfg.set("format", "grib");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.grib1");

    metadata::Collection mdc;
    mdc.push_back(f.import_results[0]);
    mdc[0].make_absolute();
    mdc.writeAtomically("arki-check-remove-test.md");

    {
        runtime::tests::CatchOutput co;
        int res = wcallchecked(run_cmdline<runtime::ArkiCheck>({ "arki-check", "--remove=arki-check-remove-test.md", "testds" }));
        wassert(actual_file(co.file_stderr.name()).contents_equal({}));
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds: 1 data would be deleted"
        }));
        wassert(actual(res) == 0);
    }

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.query_results({1, 0, 2}));

    {
        runtime::tests::CatchOutput co;
        int res = wcallchecked(run_cmdline<runtime::ArkiCheck>({ "arki-check", "--verbose", "--fix", "--remove=arki-check-remove-test.md", "testds" }));
        wassert(actual_file(co.file_stderr.name()).contents_equal({
            "testds: 1 data deleted"
        }));
        wassert(actual_file(co.file_stdout.name()).contents_equal({}));
        wassert(actual(res) == 0);
    }

    wassert(f.query_results({1, 2}));

    auto state = f.scan_state();
    wassert(actual(state.size()) == 3u);

    wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::SEGMENT_OK);
    wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_DELETED);
    wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::SEGMENT_OK);
});

add_method("tar_archives", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    f.cfg.set("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.odimh5/00.odimh5");
    f.import("inbound/fixture.odimh5/01.odimh5");
    f.import("inbound/fixture.odimh5/02.odimh5");

    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.set("archive age", "1");
    f.test_reread_config();
    f.repack();

    sys::rename("testds/.archive/last", "testds/.archive/2007");

    wassert(actual_file("testds/.archive/2007/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--tar", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: should be tarred",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--tar", "--offline", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: should be tarred",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--tar", "--online", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds:2007/07-08.odimh5: should be tarred",
            "testds:2007/10-09.odimh5: should be tarred",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--tar", "testds", "-f" });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: tarred",
        }));
        wassert(actual(res) == 0);
    }

    wassert(actual_file("testds/.archive/2007/2007/07-07.odimh5").not_exists());
    wassert(actual_file("testds/.archive/2007/2007/07-07.odimh5.tar").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.ensure_localds_clean(3, 3, false));
    wassert(f.query_results({1, 0, 2}));
});

add_method("zip_archives", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    skip_unless_libzip();
    skip_unless_libarchive();

    f.cfg.set("format", "odimh5");
    f.test_reread_config();
    f.clean_and_import("inbound/fixture.odimh5/00.odimh5");
    f.import("inbound/fixture.odimh5/01.odimh5");
    f.import("inbound/fixture.odimh5/02.odimh5");

    auto o = dataset::SessionTime::local_override(1184018400); // date +%s --date="2007-07-10"
    f.cfg.set("archive age", "1");
    f.test_reread_config();
    f.repack();

    sys::rename("testds/.archive/last", "testds/.archive/2007");

    wassert(actual_file("testds/.archive/2007/2007/07-07.odimh5").exists());
    wassert(actual_file("testds/2007/07-08.odimh5").exists());
    wassert(actual_file("testds/2007/10-09.odimh5").exists());

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--zip", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: should be zipped",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--zip", "--offline", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: should be zipped",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--zip", "--online", "testds", });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds:2007/07-08.odimh5: should be zipped",
            "testds:2007/10-09.odimh5: should be zipped",
        }));
        wassert(actual(res) == 0);
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiCheck>({ "arki-check", "--zip", "testds", "-f" });
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "testds.archives.2007:2007/07-07.odimh5: zipped",
        }));
        wassert(actual(res) == 0);
    }

    wassert(f.archived_segment_exists("2007/2007/07-07.odimh5", {".zip"}));
    wassert(f.online_segment_exists("2007/07-08.odimh5", {""}));
    wassert(f.online_segment_exists("2007/10-09.odimh5", {""}));

    wassert(f.ensure_localds_clean(3, 3));
    wassert(f.ensure_localds_clean(3, 3, false));
    wassert(f.query_results({1, 0, 2}));
});

}

}
