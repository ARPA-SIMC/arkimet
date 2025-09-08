#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/tests.h"
#include "reader.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::metadata;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_reader");

void Tests::register_tests()
{
    add_method("noop", []() noexcept {
        // TODO: move tests from Metadata to here
    });
}

} // namespace
