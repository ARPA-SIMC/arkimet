#include "arki/core/tests.h"
#include "lexer.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::matcher::reftime;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_reftime_lexer");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}

