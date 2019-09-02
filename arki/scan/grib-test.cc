#include "arki/metadata/tests.h"
#include "arki/core/file.h"
#include "arki/scan/grib.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/run.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_scan_grib");

void Tests::register_tests() {

// Test validation
add_method("validation", [] {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::grib::validator();

    sys::File fd("inbound/test.grib1", O_RDONLY);
    v.validate_file(fd, 0, 7218);
    v.validate_file(fd, 7218, 34960);
    v.validate_file(fd, 42178, 2234);

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7217));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 7219));
    wassert_throws(std::runtime_error, v.validate_file(fd, 7217, 34961));
    wassert_throws(std::runtime_error, v.validate_file(fd, 42178, 2235));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 0));
    wassert_throws(std::runtime_error, v.validate_file(fd, 44412, 10));

    fd.close();

    metadata::TestCollection mdc("inbound/test.grib1");
    buf = mdc[0].get_data().read();

    wassert(v.validate_buf(buf.data(), buf.size()));
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));
    wassert_throws(std::runtime_error, v.validate_buf(buf.data(), buf.size()-1));
});

#if 0
// Check opening very long GRIB files for scanning
// TODO: needs skipping of there are no holes
// FIXME: cannot reenable it because currently there is no interface for
// opening without scanning, and eccodes takes a long time to skip all those
// null bytes
add_method("bigfile", [] {
    scan::Grib scanner;
    int fd = open("bigfile.grib1", O_WRONLY | O_CREAT, 0644);
    ensure(fd != -1);
    ensure(ftruncate(fd, 0xFFFFFFFF) != -1);
    close(fd);
    wassert(scanner.test_open("bigfile.grib1"));
});
#endif

}

}
