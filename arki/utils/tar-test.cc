#include "arki/tests/tests.h"
#include "tar.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_tar");

void Tests::register_tests() {

// Test compressing data that do not compress
add_method("empty", [] {
    sys::File out("test.tar", O_WRONLY | O_CREAT);
    TarOutput tout(out);
    tout.append("test.txt", "test");
    tout.end();
    out.close();

    wassert(actual(system("tar tf test.tar > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == "antani");
});

}

}
