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

add_method("empty", [] {
});

}

}
