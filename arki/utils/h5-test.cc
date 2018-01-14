#include "arki/tests/tests.h"
#include "h5.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils::h5;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_h5");

void Tests::register_tests() {

add_method("read", [] {
    static const char* fname = "inbound/odimh5/IMAGE_CAPPI_v20.h5";
    MuteErrors h5e;
    File f(H5Fopen(fname, H5F_ACC_RDONLY, H5P_DEFAULT));
    wassert(actual((hid_t)f) >= 0);

    Group group(H5Gopen(f, "/dataset1/data1/quality1/how", H5P_DEFAULT));
    wassert(actual((hid_t)group) >= 0);

    Attr attr(H5Aopen(group, "task", H5P_DEFAULT));
    wassert(actual((hid_t)attr) >= 0);

    string val;
    wassert(actual(attr.read_string(val)).istrue());
    wassert(actual(val) == "Anna Fornasiero");
});

}

}
