#include "arki/tests/tests.h"
#include "seqfile.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_seqfile");

void Tests::register_tests()
{

    add_method("empty", []() noexcept {});
}

} // namespace
