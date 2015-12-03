#include <arki/tests/tests.h>
#include "arki/metadata.h"
#include "xargs.h"
#include "collection.h"
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::utils;

struct arki_metadata_xargs_shar {
    metadata::Collection mdc;

    arki_metadata_xargs_shar()
    {
        scan::scan("inbound/test.grib1", mdc);
    }
};
TESTGRP(arki_metadata_xargs);

namespace {
inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}
}

// Test what happens with children's stdin
template<> template<>
void to::test<1>()
{
    sys::unlink_ifexists("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("wc -c >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = utils::files::read_file("tmp-xargs");
    // Nothing should be send to the child's stdin
    wassert(actual(out) == "0\n");
}

// Test that env vars are set
template<> template<>
void to::test<2>()
{
    sys::unlink_ifexists("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("set | grep ARKI_XARGS_ >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = utils::files::read_file("tmp-xargs");
    wassert(actual(out).contains("ARKI_XARGS_FILENAME="));
    wassert(actual(out).matches("ARKI_XARGS_FORMAT=['\"]?GRIB1['\"]?\n"));
    wassert(actual(out).matches("ARKI_XARGS_COUNT=['\"]?10['\"]?\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_START='2007-07-08 13:00:00Z'\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_END='2007-07-08 13:00:00Z'\n"));
}

}

// vim:set ts=4 sw=4:
