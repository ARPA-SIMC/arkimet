#include "arki/dataset/tests.h"
#include "arki/runtime/tests.h"
#include "arki/exceptions.h"
#include "arki-query.h"

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
} test("arki_runtime_arkiquery");

void Tests::register_tests() {

add_method("postproc", [](Fixture& f) {
    using runtime::tests::run_cmdline;
    f.clean_and_import("inbound/test.grib1");

    runtime::tests::CatchOutput co;
    int res = run_cmdline(runtime::arki_query, {
        "arki-query",
        "--postproc=checkfiles",
        "",
        "testds",
        "--postproc-data=/dev/null",
    });
    wassert(actual(res) == 0);

    wassert(actual(sys::read_file(co.file_stdout.name())) == "/dev/null\n");
    wassert(actual(sys::read_file(co.file_stderr.name())) == "");
});

}

}
