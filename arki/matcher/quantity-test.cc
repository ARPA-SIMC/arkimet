#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/quantity.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_quantity");

void Tests::register_tests() {

add_method("odim", [] {
    Metadata md;
    arki::tests::fill(md);

	ensure_matches("quantity:", md);
	ensure_matches("quantity:a", md);
	ensure_matches("quantity:b", md);
	ensure_matches("quantity:c", md);
	ensure_matches("quantity:a,b", md);
	ensure_matches("quantity:a,b,c", md);

	ensure_not_matches("quantity:x", md);
	ensure_not_matches("quantity:a,b,c,d", md);
});

}

}
