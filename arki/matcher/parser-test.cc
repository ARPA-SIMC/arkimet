#include "arki/matcher/parser.h"
#include "arki/matcher/tests.h"

using namespace std;
using namespace arki::tests;
using namespace arki::core;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} tests("arki_matcher_parser");

void Tests::register_tests()
{

    add_method("empty", []() noexcept {});
}

} // namespace
