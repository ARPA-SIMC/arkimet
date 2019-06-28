#include "arki/tests/tests.h"
#include "arki/core/cfg.h"
#include "arki/runtime/config.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::runtime;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_runtime_config");

void Tests::register_tests() {

// Test restrict functions
add_method("restrict", [] {
    core::cfg::Section empty_cfg;
    core::cfg::Section allowed_cfg;
    core::cfg::Section unallowed_cfg;
    allowed_cfg.set("restrict", "c, d, e, f");
    unallowed_cfg.set("restrict", "d, e, f");

    // With no restrictions, everything should be allowed
    Restrict r1("");
    wassert_true(r1.is_allowed(""));
    wassert_true(r1.is_allowed("a,b,c"));
    wassert_true(r1.is_allowed(empty_cfg));
    wassert_true(r1.is_allowed(allowed_cfg));
    wassert_true(r1.is_allowed(unallowed_cfg));

    // Normal restrictions
    Restrict r2("a, b,c");
    wassert_false(r2.is_allowed(""));
    wassert_true(r2.is_allowed("a"));
    wassert_true(r2.is_allowed("a, b"));
    wassert_true(r2.is_allowed("c, d, e, f"));
    wassert_false(r2.is_allowed("d, e, f"));
    wassert_false(r2.is_allowed(empty_cfg));
    wassert_true(r2.is_allowed(allowed_cfg));
    wassert_false(r2.is_allowed(unallowed_cfg));
});

}

}
