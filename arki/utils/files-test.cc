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

void Tests::register_tests()
{

    // Test dontpack flagfile creation
    add_method("dontpack", [] {
        sys::rmtree_ifexists("commontest");
        std::filesystem::create_directory("commontest");

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

    add_method("pathwalk_changed_during_iteration", [] {
        std::vector<std::string> names = {"file1", "file2", "file3"};
        sys::Tempdir path;
        for (const auto& fname : names)
            sys::FileDescriptor fd(
                path.openat(fname.c_str(), O_CREAT | O_WRONLY));

        unsigned count = 0;
        PathWalk walk(path.path(), [&](const std::filesystem::path&,
                                       sys::Path::iterator&, struct stat&) {
            if (count == 0)
                for (const auto& fname : names)
                    path.unlinkat(fname.c_str());
            ++count;
            return true;
        });
        walk.walk();
        wassert(actual(count) == 1u);
    });
}

} // namespace
