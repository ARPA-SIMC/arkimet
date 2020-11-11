#include "arki/metadata/tests.h"
#include "arki/scan/netcdf.h"
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
} test("arki_scan_netcdf");

void Tests::register_tests() {

add_method("validator_3", [] {
    sys::File in("inbound/netcdf/empty1-3.nc", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    validator.validate_file(in, 0, 1636);
});

add_method("validator_4", [] {
    sys::File in("inbound/netcdf/empty1-4.nc", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    validator.validate_file(in, 0, 9487);
});

add_method("validator_5", [] {
    sys::File in("inbound/netcdf/empty1-5.nc", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    validator.validate_file(in, 0, 1988);
});

add_method("validator_6", [] {
    sys::File in("inbound/netcdf/empty1-6.nc", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    validator.validate_file(in, 0, 1648);
});

add_method("validator_7", [] {
    sys::File in("inbound/netcdf/empty1-7.nc", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    validator.validate_file(in, 0, 9545);
});

add_method("validator_nonnc", [] {
    sys::File in("inbound/test.grib1", O_RDONLY);
    const scan::Validator& validator = scan::netcdf::validator();
    wassert_throws(std::runtime_error, validator.validate_file(in, 0, 44412));
});


}

}

