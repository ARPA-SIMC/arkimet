#include "arki/tests/tests.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <system_error>

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
    wassert(removeDontpackFlagfile(name));
    wassert(actual(hasDontpackFlagfile(name)).isfalse());
});

// Test resolve_path
add_method("resolve_path", [] {
    using namespace arki::utils::files;
    std::filesystem::path basedir, relpath;

    resolve_path(".", basedir, relpath);
    wassert(actual_path(basedir) == std::filesystem::current_path());
    wassert(actual(relpath) == ".");

    resolve_path("/tmp/foo", basedir, relpath);
    wassert(actual(basedir) == "");
    wassert(actual(relpath) == "/tmp/foo");

    resolve_path("foo/bar/../baz", basedir, relpath);
    wassert(actual_path(basedir) == std::filesystem::current_path());
    wassert(actual(relpath) == "foo/baz");
});

}

}
