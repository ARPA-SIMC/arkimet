#include "arki/tests/tests.h"
#include "lock.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_lock");

void Tests::register_tests() {}

} // namespace
