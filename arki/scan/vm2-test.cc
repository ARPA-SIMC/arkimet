#include "arki/metadata/tests.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/value.h"
#include "arki/scan/vm2.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>

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
} test("arki_scan_vm2");

void Tests::register_tests() {

// Scan a well-known vm2 sample
add_method("scan", []() {
    scan::Vm2 scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/test.vm2", mds.inserter_func());
    wassert(actual(mds.size()) == 4u);
    Metadata& md = mds[0];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test.vm2", 0, 34));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(227)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:00Z"));
    wassert(actual(md).contains("value", "1.2,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    auto buf = md.get_data().read();
    wassert(actual(buf.size()) == 34u);
    wassert(actual(string((const char*)buf.data(), 34)) == "198710310000,1,227,1.2,,,000000000");
});

// Scan a well-known vm2 sample (with seconds)
add_method("scan_seconds", []() {
    scan::Vm2 scanner;
    metadata::Collection mds;
    scanner.test_scan_file("inbound/test.vm2", mds.inserter_func());
    wassert(actual(mds.size()) == 4u);
    Metadata& md = mds[1];

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test.vm2", 35, 35));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(228)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:30Z"));
    wassert(actual(md).contains("value", ".5,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    auto buf = md.get_data().read();
    wassert(actual(buf.size()) == 35u);
    wassert(actual(string((const char*)buf.data(), 35)) == "19871031000030,1,228,.5,,,000000000");
});

add_method("validate", []() {
    Metadata md;
    vector<uint8_t> buf;

    const scan::Validator& v = scan::vm2::validator();

    sys::File fd("inbound/test.vm2", O_RDONLY);
    wassert(v.validate_file(fd, 0, 35));
    wassert(v.validate_file(fd, 0, 34));
    wassert(v.validate_file(fd, 35, 35));
    wassert(v.validate_file(fd, 35, 36));

    wassert_throws(std::runtime_error, v.validate_file(fd, 1, 35));
    wassert_throws(std::runtime_error, v.validate_file(fd, 0, 36));
    wassert_throws(std::runtime_error, v.validate_file(fd, 34, 34));
    wassert_throws(std::runtime_error, v.validate_file(fd, 36, 34));

    fd.close();

    metadata::TestCollection mdc("inbound/test.vm2");
    mdc[0].unset(TYPE_VALUE);
    buf = mdc[0].get_data().read();

    v.validate_buf(buf.data(), buf.size());
    wassert_throws(std::runtime_error, v.validate_buf((const char*)buf.data()+1, buf.size()-1));

    std::ifstream in("inbound/test.vm2");
    std::string line;
    while (std::getline(in, line)) {
        line += "\n";
        wassert(v.validate_buf(line.c_str(), line.size()));
    }
});

// Scan and reconstruct a VM2 sample
add_method("reconstruct", []() {
    const types::Value* value;
    vector<uint8_t> buf;

    metadata::TestCollection mdc("inbound/test.vm2");

    value = mdc[0].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[0], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");

    value = mdc[1].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[1], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "19871031000030,1,228,.5,,,000000000");
});

// Scan a corrupted VM2
add_method("corrupted", []() {
    system("cp inbound/test.vm2 inbound/test-corrupted.vm2");
    system("dd if=/dev/zero of=inbound/test-corrupted.vm2 bs=1 seek=71 count=33 conv=notrunc 2>/dev/null");

    metadata::TestCollection mdc("inbound/test-corrupted.vm2");
    wassert(actual(mdc.size()) == 3u);

    // Check the source info
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 0, 34));
    wassert(actual(mdc[1].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 35, 35));
    wassert(actual(mdc[2].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/test-corrupted.vm2", 105, 32));

    system("rm inbound/test-corrupted.vm2");
});

add_method("issue237", [] {
    metadata::TestCollection mdc("inbound/issue237.vm2", true);
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("vm2", sys::abspath("."), "inbound/issue237.vm2", 0, 36));

    auto data = mdc[0].get_data().read();
    wassert(actual(data.size()) == 36u);
    wassert(actual(std::string((const char*)data.data(), data.size())) == "20201031230000,12865,158,9.409990,,,");

    auto value = mdc[0].get<types::Value>();
    auto buf = scan::Vm2::reconstruct(mdc[0], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "202010312300,12865,158,9.409990,,,");
});

}
}
