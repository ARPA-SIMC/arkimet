#include "registry.h"
#include "arki/tests/tests.h"
#include "arki/types/source/blob.h"
#include "arki/metadata/collection.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_reader_registry");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}

