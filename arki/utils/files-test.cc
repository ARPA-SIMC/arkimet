#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::utils::files;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_files");

void Tests::register_tests() {

// Test dontpack flagfile creation
add_method("dontpack", [] {
    sys::rmtree_ifexists("commontest");
    sys::mkdir_ifmissing("commontest");

    string name = "commontest";
    wassert(actual(hasDontpackFlagfile(name)).isfalse());
    wassert(createDontpackFlagfile(name));
    wassert(actual(hasDontpackFlagfile(name)).istrue());
    try {
        createNewDontpackFlagfile(name);
        ensure(false);
    } catch (...) {
    }
    wassert(removeDontpackFlagfile(name));
    wassert(actual(hasDontpackFlagfile(name)).isfalse());
    wassert(createNewDontpackFlagfile(name));
    wassert(actual(hasDontpackFlagfile(name)).istrue());
    wassert(removeDontpackFlagfile(name));
    wassert(actual(hasDontpackFlagfile(name)).isfalse());
});

// Test resolve_path
add_method("resolve_path", [] {
    using namespace arki::utils::files;
    string basedir, relname;

    resolve_path(".", basedir, relname);
    wassert(actual(basedir) == sys::abspath("."));
    wassert(actual(relname) == ".");

    resolve_path("/tmp/foo", basedir, relname);
    wassert(actual(basedir) == "");
    wassert(actual(relname) == "/tmp/foo");

    resolve_path("foo/bar/../baz", basedir, relname);
    wassert(actual(basedir) == sys::abspath("."));
    wassert(actual(relname) == "foo/baz");
});

// Test format_from_ext
add_method("format_from_ext", [] {
    using namespace arki::utils::files;

    wassert(actual(format_from_ext("test.grib")) == "grib");
    wassert(actual(format_from_ext("test.grib1")) == "grib");
    wassert(actual(format_from_ext("test.grib2")) == "grib");
    wassert(actual(format_from_ext("test.bufr")) == "bufr");
#ifdef HAVE_HDF5
    wassert(actual(format_from_ext("test.h5")) == "odimh5");
    wassert(actual(format_from_ext("test.hdf5")) == "odimh5");
    wassert(actual(format_from_ext("test.odim")) == "odimh5");
    wassert(actual(format_from_ext("test.odimh5")) == "odimh5");
#endif

});

}

}
