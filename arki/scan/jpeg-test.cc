#include "arki/metadata/tests.h"
#include "arki/scan/jpeg.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_jpeg");

void Tests::register_tests() {

add_method("validator", [] {
    sys::File in("inbound/jpeg/autumn.jpg", O_RDONLY);
    const scan::Validator& validator = scan::jpeg::validator();
    validator.validate_file(in, 0, 94701);

    std::string buf = sys::read_file("inbound/jpeg/autumn.jpg");
    validator.validate_buf(buf.data(), 94701);
});

}

}
