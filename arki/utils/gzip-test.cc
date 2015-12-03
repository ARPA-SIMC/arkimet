#include "arki/tests/tests.h"
#include "gzip.h"

using namespace std;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("read", []() {
            system("echo testtest | gzip > test.gz");
            gzip::File out("test.gz", "r");
            char buf[9];
            out.read_all_or_throw(buf, 8);
            buf[8] = 0;
            wassert(actual(buf) == "testtest");
        });
    }
} test("arki_utils_gzip");

}
