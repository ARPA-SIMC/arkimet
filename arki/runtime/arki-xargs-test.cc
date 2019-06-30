#include "arki/tests/tests.h"
#include "arki/runtime/tests.h"
#include "arki-xargs.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
};

Tests test("arki_runtime_arkixargs");

void Tests::register_tests() {

add_method("default_size", [] {
    using runtime::tests::run_cmdline;

    sys::write_file("testscript.sh", std::string(R"(#!/bin/sh -e
test "$ARKI_XARGS_FORMAT" = GRIB
test "$ARKI_XARGS_COUNT" -eq 1
test -n "$ARKI_XARGS_TIME_START"
test -n "$ARKI_XARGS_TIME_END"
test -r "$1"
test -s "$1"
)"), (mode_t)0755);

    metadata::TestCollection mds("inbound/test.grib1");
    sys::File fd("test.md", O_RDWR | O_CREAT | O_TRUNC);
    for (auto& md: mds)
    {
        md->makeInline();
        md->write(fd);
    }
    fd.lseek(0);
    runtime::tests::CatchOutput co(std::move(fd));
    int res = run_cmdline<runtime::ArkiXargs>({"arki-xargs", "-n", "1", "--", "./testscript.sh"});
    wassert(actual_file(co.file_stderr.name()).contents_equal({}));
    wassert(actual_file(co.file_stdout.name()).contents_equal({}));
    wassert(actual(res) == 0);
});

add_method("large_size", [] {
    using runtime::tests::run_cmdline;

    sys::write_file("testscript.sh", std::string(R"(#!/bin/sh -e
test "$ARKI_XARGS_FORMAT" = GRIB
test "$ARKI_XARGS_COUNT" -ge 1
test -n "$ARKI_XARGS_TIME_START"
test -n "$ARKI_XARGS_TIME_END"
test -r "$1"
test -s "$1"
FILESIZE=$(stat -c%s "$1")
test "$FILESIZE" -le 100000
)"), (mode_t)0755);

    metadata::TestCollection mds("inbound/test.grib1");
    sys::File fd("test.md", O_RDWR | O_CREAT | O_TRUNC);
    for (auto& md: mds)
    {
        md->makeInline();
        md->write(fd);
    }
    fd.lseek(0);
    runtime::tests::CatchOutput co(std::move(fd));
    int res = run_cmdline<runtime::ArkiXargs>({"arki-xargs", "-s", "100000", "--", "./testscript.sh"});
    wassert(actual_file(co.file_stderr.name()).contents_equal({}));
    wassert(actual_file(co.file_stdout.name()).contents_equal({}));
    wassert(actual(res) == 0);
});

}

}

