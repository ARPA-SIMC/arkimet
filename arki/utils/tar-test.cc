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
    std::vector<uint8_t> bindata = { 0, 1, 2, 3, 4, 5, 0x7f, 0xff };
    sys::File out("test.tar", O_WRONLY | O_CREAT);
    TarOutput tout(out);
    wassert(actual(tout.append("test1.txt", "test1")) == 512);
    wassert(actual(tout.append("test2.bin", bindata)) == 512 * 3);
    tout.end();
    out.close();

    wassert(actual(system("tar tf test.tar > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == "test1.txt\ntest2.bin\n");
    wassert(actual(system("tar xf test.tar")) == 0);
    wassert(actual(sys::read_file("test1.txt")) == "test1");
    wassert(actual(sys::read_file("test2.bin")) == std::string("\0\1\2\3\4\5\x7f\xff", 8));
});

}

}
