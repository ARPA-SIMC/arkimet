#include "arki/metadata/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/scan/bufr.h"
#include "arki/utils/sys.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"

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
} test("arki_scan_bufr");

void Tests::register_tests() {

// Test validation
add_method("validate", [] {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::bufr::validator();

    sys::File fd("inbound/test.bufr", O_RDONLY);
    v.validate_file(fd, 0, 194);
    v.validate_file(fd, 194, 220);
    v.validate_file(fd, 414, 220);

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 193));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 193));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 195));
    wassert_throws(std::runtime_error, v.validate_file(fd, 193, 221));
    wassert_throws(std::runtime_error, v.validate_file(fd, 414, 221));
    wassert_throws(std::runtime_error, v.validate_file(fd, 634, 0));
    wassert_throws(std::runtime_error, v.validate_file(fd, 634, 10));
    fd.close();

    metadata::TestCollection mdc("inbound/test.bufr");
    buf = mdc[0].get_data().read();

    wassert(v.validate_data(mdc[0].get_data()));
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));
    wassert_throws(std::runtime_error, v.validate_buf(buf.data(), buf.size()-1));
});

}

}
