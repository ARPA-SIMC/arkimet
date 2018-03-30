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

add_method("size_with_length", [] {
    wassert(actual(PaxHeader::size_with_length(8)) == 9u);
    wassert(actual(PaxHeader::size_with_length(9)) == 11u);
    wassert(actual(PaxHeader::size_with_length(10)) == 12u);
    wassert(actual(PaxHeader::size_with_length(97)) == 99u);
    wassert(actual(PaxHeader::size_with_length(98)) == 101u);
    wassert(actual(PaxHeader::size_with_length(99)) == 102u);
    wassert(actual(PaxHeader::size_with_length(996)) == 999u);
    wassert(actual(PaxHeader::size_with_length(997)) == 1001u);
    wassert(actual(PaxHeader::size_with_length(9999992)) == 9999999u);
    wassert(actual(PaxHeader::size_with_length(9999993)) == 10000001u);
    wassert(actual(PaxHeader::size_with_length(999999990)) == 999999999u);
    wassert(actual(PaxHeader::size_with_length(999999991)) == 1000000001u);
    wassert(actual(PaxHeader::size_with_length(9999999989)) == 9999999999u);
    wassert(actual(PaxHeader::size_with_length(9999999990)) == 10000000001u);
});

add_method("plain", [] {
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

add_method("pax", [] {
    sys::File out("test.tar", O_WRONLY | O_CREAT);
    TarOutput tout(out);
    PaxHeader pax;
    pax.append("path", "test.txt");
    tout.append(pax);
    wassert(actual(tout.append("test.bin", "test")) == 512 * 3);
    tout.end();
    out.close();

    wassert(actual(system("tar tf test.tar > test.out")) == 0);
    wassert(actual(sys::read_file("test.out")) == "test.txt\n");
    wassert(actual(system("tar xf test.tar")) == 0);
    wassert(actual(sys::read_file("test.txt")) == "test");
    wassert(actual_file("@PaxHeader").not_exists());
});

}

}
