#include "arki/tests/tests.h"
#include "gzip.h"

using namespace std;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_utils_gzip");

void Tests::register_tests() {

add_method("read", []() {
    system("echo testtest | gzip > test.gz");
    gzip::File out("test.gz", "r");
    char buf[9];
    out.read_all_or_throw(buf, 8);
    buf[8] = 0;
    wassert(actual(buf) == "testtest");
});

add_method("read_all", []() {
    system("echo testtest | gzip > test.gz");
    gzip::File out("test.gz", "r");
    auto buf = out.read_all();
    wassert(actual(buf) == "testtest\n");
});

}

}
