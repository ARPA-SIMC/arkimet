#include "arki/core/tests.h"
#include "arki/metadata.h"
#include "xargs.h"
#include "collection.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

inline std::shared_ptr<Metadata> wrap(const Metadata& md)
{
    return std::shared_ptr<Metadata>(md.clone());
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_xargs");

void Tests::register_tests() {

// Test what happens with children's stdin
add_method("check_stdin", [] {
    metadata::TestCollection mdc("inbound/test.grib1");
    std::filesystem::remove("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("wc -c >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = sys::read_file("tmp-xargs");
    // Nothing should be send to the child's stdin
    wassert(actual(out) == "0\n");
});

// Test that env vars are set
add_method("check_env", [] {
    metadata::TestCollection mdc("inbound/test.grib1");
    std::filesystem::remove("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("set | grep ARKI_XARGS_ >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = sys::read_file("tmp-xargs");
    wassert(actual(out).contains("ARKI_XARGS_FILENAME="));
    wassert(actual(out).matches("ARKI_XARGS_FORMAT=['\"]?GRIB['\"]?\n"));
    wassert(actual(out).matches("ARKI_XARGS_COUNT=['\"]?10['\"]?\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_START='2007-07-08 13:00:00Z'\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_END='2007-07-08 13:00:00Z'\n"));
});

add_method("interval", [] {
    metadata::TestCollection mdc("inbound/test.grib1");
    std::filesystem::remove("tmp-xargs");
    metadata::Xargs xargs;
    xargs.set_interval("day");
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("echo $ARKI_XARGS_FILENAME >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.eat(wrap(mdc[0]));
    xargs.eat(wrap(mdc[1]));
    xargs.eat(wrap(mdc[2]));
    xargs.flush();

    string out = sys::read_file("tmp-xargs");
    wassert(actual(out).matches("([[:alnum:]/]+/arki-xargs.[[:alnum:]]+\n){3}"));
});

add_method("issue124", [] {
    sys::rmtree_ifexists("xargs_tmpdir");
    std::filesystem::create_directory("xargs_tmpdir");
    arki::tests::OverrideEnvironment oe("TMPDIR", std::filesystem::canonical("xargs_tmpdir"));

    metadata::TestCollection mdc("inbound/test.grib1");

    {
        metadata::Xargs xargs;
        xargs.command.push_back("/bin/sh");
        xargs.command.push_back("-c");
        xargs.command.push_back("cat > /dev/null");
        xargs.filename_argument = 1000; // Do not pass the file name
        xargs.eat(wrap(mdc[0]));
        xargs.eat(wrap(mdc[1]));
        xargs.eat(wrap(mdc[2]));

        // Check that xargs_tmpdir contains 1 file
        sys::Path path("xargs_tmpdir");
        std::vector<std::string> contents;
        for (auto name: path)
            if (name.d_name[0] != '.')
                contents.push_back(name.d_name);
        wassert(actual(contents.size()) == 1u);

        xargs.flush();
    }

    // Check that xargs_tmpdir contains 0 files
    sys::Path path("xargs_tmpdir");
    std::vector<std::string> contents;
    for (auto name: path)
        if (name.d_name[0] != '.')
            contents.push_back(name.d_name);
    wassert(actual(contents.size()) == 0u);
});

// Check that xargs exit withour error when the command remove the tmp file
add_method("check_tmpfile_exist", [] {
    metadata::TestCollection mdc("inbound/test.grib1");
    std::filesystem::remove("tmp-xargs");
    metadata::Xargs xargs;
    xargs.set_interval("day");
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("rm $ARKI_XARGS_FILENAME");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.eat(wrap(mdc[0]));
    xargs.eat(wrap(mdc[1]));
    xargs.eat(wrap(mdc[2]));
    xargs.flush();
});
}

}
