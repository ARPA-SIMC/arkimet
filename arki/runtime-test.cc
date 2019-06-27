#include "config.h"
#include <fstream>
#include <algorithm>
#include "arki/tests/tests.h"
#include "arki/metadata/collection.h"
#include "arki/utils/sys.h"
#include "arki/runtime.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/dispatch.h"
#include "arki/dataset/file.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_runtime");

struct CollectProcessor : public runtime::DatasetProcessor
{
    metadata::Collection mdc;

    void process(dataset::Reader& ds, const std::string& name) override {
        mdc.add(ds, Matcher());
    }
    std::string describe() const { return "[test]CollectProcessor"; }
};

void Tests::register_tests() {

add_method("files", [] {
    // Reproduce https://github.com/ARPAE-SIMC/arkimet/issues/19

    utils::sys::write_file("import.lst", "grib:inbound/test.grib1\n");
    utils::sys::write_file("config", "[error]\ntype=discard\n");

    runtime::ScanCommandLine opts("arki-scan");

    const char* argv[] = { "arki-scan", "--dispatch=config", "--dump", "--status", "--summary", "--files=import.lst", nullptr };
    int argc = sizeof(argv) / sizeof(argv[0]) - 1;
    wassert(actual(opts.parse(argc, argv)).isfalse());

    runtime::init();
});

}

}
