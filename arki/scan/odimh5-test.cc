#include "arki/metadata/tests.h"
#include "arki/scan/odimh5.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/types/source.h"
#include "arki/utils/sys.h"
#include <iostream>

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
} test("arki_scan_odimh5");

void Tests::register_tests() {

// TODO: check the validator
add_method("empty", [] {
});

}

}
