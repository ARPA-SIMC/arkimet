#include "arki/dataset/tests.h"
#include "arki/runtime/tests.h"
#include "arki/libconfig.h"
#include "arki-scan.h"

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
            type=ondisk2
            step=daily
            postprocess=cat,echo,say,checkfiles,error,outthenerr
        )");
        /*
        if (td.format == "vm2")
            cfg.setValue("smallfiles", "true");
        import_all_packed(td);
        */
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
} test("arki_runtime_arkiscan");

void Tests::register_tests() {

add_method("scan_grib", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    runtime::tests::CatchOutput co;
    int res = run_cmdline(runtime::arki_scan, {
        "arki-scan",
        "--yaml",
        "inbound/test.grib1",
    });
    wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    wassert(actual(res) == 0);
    wassert(actual(sys::read_file(co.file_stdout.name())).matches("\nOrigin:"));
});

add_method("scan_bufr", [](Fixture& f) {
    using runtime::tests::run_cmdline;
#ifndef HAVE_DBALLE
    throw TestSkipped("BUFR support not available");
#endif
    runtime::tests::CatchOutput co;
    int res = run_cmdline(runtime::arki_scan, {
        "arki-scan",
        "--yaml",
        "inbound/test.bufr",
    });
    wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    wassert(actual(res) == 0);
    wassert(actual(sys::read_file(co.file_stdout.name())).matches("\nOrigin:"));
});

add_method("scan_bufr_multiple", [](Fixture& f) {
    using runtime::tests::run_cmdline;
#ifndef HAVE_DBALLE
    throw TestSkipped("BUFR support not available");
#endif
    runtime::tests::CatchOutput co;
    int res = run_cmdline(runtime::arki_scan, {
        "arki-scan",
        "--yaml",
        "inbound/test.bufr",
        "inbound/ship.bufr",
    });
    wassert(actual(sys::read_file(co.file_stderr.name())) == "");
    wassert(actual(res) == 0);
    wassert(actual(sys::read_file(co.file_stdout.name())).matches("\nOrigin:"));
});

add_method("scan_metadata", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    metadata::TestCollection mdc("inbound/fixture.grib1");

    mdc.writeAtomically("test.metadata");
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_scan, {
            "arki-scan",
            "--yaml",
            "test.metadata",
        });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())).matches("\nOrigin:"));
    }

    mdc.writeAtomically("test.foo");
    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_scan, {
            "arki-scan",
            "--yaml",
            "metadata:test.foo",
        });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "");
        wassert(actual(res) == 0);
        wassert(actual(sys::read_file(co.file_stdout.name())).matches("\nOrigin:"));
    }
});

add_method("scan_stdin", [](Fixture& f) {
    using runtime::tests::run_cmdline;

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_scan, {
            "arki-scan",
            "--yaml",
            "-",
        });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "file - does not exist\n");
        wassert(actual(res) == 1);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_scan, {
            "arki-scan",
            "--json",
            "bufr:-",
        });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "file - does not exist\n");
        wassert(actual(res) == 1);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
    }

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline(runtime::arki_scan, {
            "arki-scan",
            "--dispatch=testds/config",
            "-",
        });
        wassert(actual(sys::read_file(co.file_stderr.name())) == "file - does not exist\n");
        wassert(actual(res) == 1);
        wassert(actual(sys::read_file(co.file_stdout.name())) == "");
    }
});

}

}
