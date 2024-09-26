#include "arki/types/tests.h"
#include "arki/segment.h"
#include "arki/metadata/collection.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_collection");

void Tests::register_tests() {

add_method("empty", []() noexcept {
});

}

}
