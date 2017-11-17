#include "arki/dataset/tests.h"
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


struct CatchOutput
{
    sys::File file_stdin;
    sys::File file_stdout;
    sys::File file_stderr;
    int orig_stdin;
    int orig_stdout;
    int orig_stderr;

    CatchOutput()
        : file_stdin(File::mkstemp(".")),
          file_stdout(File::mkstemp(".")),
          file_stderr(File::mkstemp("."))
    {
        orig_stdin = save(file_stdin, 0);
        orig_stdout = save(file_stdout, 1);
        orig_stderr = save(file_stderr, 2);
    }

    ~CatchOutput()
    {
        restore(orig_stdin, 0);
        restore(orig_stdout, 1);
        restore(orig_stderr, 2);
    }

    int save(int src, int tgt)
    {
        int res = ::dup(tgt);
        if (res == -1)
            throw_system_error("cannot dup file descriptor " + to_string(tgt));
        if (::dup2(src, tgt) == -1)
            throw_system_error("cannot dup2 file descriptor " + to_string(src) + " to " + to_string(tgt));
        return res;
    }

    void restore(int src, int tgt)
    {
        if (::dup2(src, tgt) == -1)
            throw_system_error("cannot dup2 file descriptor " + to_string(src) + " to " + to_string(tgt));
        if (::close(src) == -1)
            throw_system_error("cannot close saved file descriptor " + to_string(src));
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
} test("arki_runtime_arkiquery");

void Tests::register_tests() {

add_method("postproc", [](Fixture& f) {
    f.clean_and_import("inbound/test.grib1");

    const char* argv[] = {
        "arki-query",
        "--postproc=checkfiles",
        "",
        "testds",
        "--postproc-data=/dev/null",
        nullptr,
    };
    int argc = sizeof(argv) / sizeof(argv[0]) - 1;

    CatchOutput co;
    wassert(actual(runtime::arki_query(argc, argv)) == 0);

    wassert(actual(sys::read_file(co.file_stdout.name())) == "/dev/null\n");
    wassert(actual(sys::read_file(co.file_stderr.name())) == "");
});

}

}

