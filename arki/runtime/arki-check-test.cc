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
