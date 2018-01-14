#include "arki/tests/tests.h"
#include "arki/metadata.h"
#include "targetfile.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

void fill(Metadata& md)
{
    md.set("origin", "GRIB1(1, 2, 3)");
    md.set("product", "GRIB1(1, 2, 3)");
    md.set("level", "GRIB1(110, 12, 13)");
    md.set("timerange", "GRIB1(0, 0s)");
    md.set("area", "GRIB(antani=-1, blinda=0)");
    md.set("proddef", "GRIB(antani=-1, blinda=0)");
    md.add_note("test note");
    md.set("run", "MINUTE(12)");
    md.set("reftime", "2007-01-02 03:04:05");
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_targetfile");

void Tests::register_tests() {

add_method("echo", [] {
    Metadata md;
    fill(md);

    Targetfile tf(R"(
targetfile['echo'] = function(format)
  return function(md)
    return format
  end
end
)");
    Targetfile::Func f = tf.get("echo:foo");
    wassert(actual(f(md)) == "foo");
});

// Test MARS expansion
add_method("mars", [] {
    Metadata md;
    fill(md);

    Targetfile tf;
    tf.loadRCFiles();
    Targetfile::Func f = tf.get("mars:foo[DATE][TIME]+[STEP].grib");
    wassert(actual(f(md)) == "foo200701020304+00.grib");
});

}

}
